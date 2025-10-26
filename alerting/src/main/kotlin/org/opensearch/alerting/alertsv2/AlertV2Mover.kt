/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alertsv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_ENABLED
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.VersionType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.concurrent.TimeUnit

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val logger = LogManager.getLogger(AlertV2Mover::class.java)

class AlertV2Mover(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val xContentRegistry: NamedXContentRegistry,
) : ClusterStateListener {
    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_ENABLED) { alertV2HistoryEnabled = it }
    }

    @Volatile private var isClusterManager = false

    private var alertV2IndexInitialized = false

    private var alertV2HistoryIndexInitialized = false

    private var alertV2HistoryEnabled = ALERT_V2_HISTORY_ENABLED.get(settings)

    private var scheduledAlertsV2CheckAndExpire: Scheduler.Cancellable? = null

    private val executorName = ThreadPool.Names.MANAGEMENT

    private val checkForExpirationInterval = TimeValue(1L, TimeUnit.MINUTES)

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onManager()
            } else {
                offManager()
            }
        }

        alertV2IndexInitialized = event.state().routingTable().hasIndex(AlertV2Indices.ALERT_V2_INDEX)
        alertV2HistoryIndexInitialized = event.state().metadata().hasAlias(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
    }

    fun onManager() {
        try {
            // try to sweep current AlertV2s for expiration immediately as we might be restarting the cluster
            moveOrDeleteAlertV2s()
            // schedule expiration checks and expirations to happen repeatedly at some interval
            scheduledAlertsV2CheckAndExpire = threadPool
                .scheduleWithFixedDelay({ moveOrDeleteAlertV2s() }, checkForExpirationInterval, executorName)
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error sweeping AlertV2s for expiration. This cannot be done until clustermanager node is restarted.",
                e
            )
        }
    }

    fun offManager() {
        scheduledAlertsV2CheckAndExpire?.cancel()
    }

    // if alertV2 history is enabled, move expired alerts to alertV2 history indices
    // if alertV2 history is disabled, permanently delete expired alerts
    private fun moveOrDeleteAlertV2s() {
        logger.info("in move or delete alert v2")
        if (!areAlertV2IndicesPresent()) {
            return
        }

        scope.launch {
            val expiredAlerts = searchForExpiredAlerts()

            var copyResponse: BulkResponse? = null
            val deleteResponse: BulkResponse?
            if (!alertV2HistoryEnabled) {
                logger.info("alert history disabled")
                deleteResponse = deleteExpiredAlerts(expiredAlerts)
            } else {
                logger.info("alert history enabled")
                copyResponse = copyExpiredAlerts(expiredAlerts)
                deleteResponse = deleteExpiredAlertsThatWereCopied(copyResponse)
            }
            checkForFailures(copyResponse)
            checkForFailures(deleteResponse)
        }
    }

    private suspend fun searchForExpiredAlerts(): List<AlertV2> {
        // first collect all active alerts
        val allAlertsSearchQuery = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.matchAllQuery())
            .size(10000)
            .version(true)
        val activeAlertsRequest = SearchRequest(AlertV2Indices.ALERT_V2_INDEX)
            .source(allAlertsSearchQuery)
        val searchAlertsResponse: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

        val allAlertV2s = mutableListOf<AlertV2>()
        searchAlertsResponse.hits.forEach { hit ->
            allAlertV2s.add(
                AlertV2.parse(alertV2ContentParser(hit.sourceRef), hit.id, hit.version)
            )
        }

        // now collect all monitorV2s
        val monitorV2sSearchQuery = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.existsQuery(MONITOR_V2_TYPE))
            .size(10000)
            .version(true)
        val monitorV2sRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
            .source(monitorV2sSearchQuery)
        val searchMonitorV2sResponse: SearchResponse = client.suspendUntil { search(monitorV2sRequest, it) }

        val monitorV2s = mutableMapOf<String, MonitorV2>()
        searchMonitorV2sResponse.hits.forEach { hit ->
            monitorV2s.put( // TODO: add monitor v2 sanity check here
                hit.id, ScheduledJob.parse(scheduledJobContentParser(hit.sourceRef), hit.id, hit.version) as MonitorV2
            )
        }

        val now = Instant.now().toEpochMilli()

        // now collect all alerts that are now expired
        val expiredAlerts = mutableListOf<AlertV2>()
        for (alertV2 in allAlertV2s) {
            val monitorV2Id = alertV2.monitorId
            val triggerV2Id = alertV2.triggerId
            val triggeredTime = alertV2.triggeredTime.toEpochMilli()

            val monitorV2 = monitorV2s[monitorV2Id]

            // if the monitor ID associated with the alert
            // no longer exists, then it must have gotten
            // deleted after this alert was generated, expire
            // the alert
            if (monitorV2 == null) {
                expiredAlerts.add(alertV2)
                continue
            }

            var triggerFound = false
            for (trigger in monitorV2.triggers) {
                if (trigger.id != triggerV2Id) continue

                // expire durations come in minutes, convert to millis
                val expireDurationMillis = trigger.expireDuration * 60 * 1000

                if (now - triggeredTime >= expireDurationMillis) {
                    expiredAlerts.add(alertV2)
                }

                triggerFound = true

                break
            }

            // if this alert's trigger was not found,
            // it's likely that the trigger's ID was
            // edited somehow, clean this alert up to
            // retain only alerts generated by triggers
            // that have a currently existing id
            if (!triggerFound) {
                expiredAlerts.add(alertV2)
            }
        }

        return expiredAlerts
    }

    private suspend fun deleteExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val deleteRequests = expiredAlerts.map {
            DeleteRequest(AlertV2Indices.ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }

        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

        return deleteResponse
    }

    private suspend fun copyExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val indexRequests = expiredAlerts.map {
            IndexRequest(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
                .source(it.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
                .id(it.id)
        }

        val copyRequest = BulkRequest().add(indexRequests)
        val copyResponse: BulkResponse = client.suspendUntil { bulk(copyRequest, it) }

        return copyResponse
    }

    private suspend fun deleteExpiredAlertsThatWereCopied(copyResponse: BulkResponse?): BulkResponse? {
        // if there were no expired alerts to copy, skip deleting anything
        if (copyResponse == null) {
            return null
        }

        val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
            DeleteRequest(AlertV2Indices.ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }
        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

        return deleteResponse
    }

    private fun checkForFailures(bulkResponse: BulkResponse?) {
        bulkResponse?.let {
            if (bulkResponse.hasFailures()) {
                val retryCause = bulkResponse.items.filter { it.isFailed }
                    .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    ?.failure?.cause
                throw RuntimeException(
                    "Failed to move or delete alert v2s: " +
                        bulkResponse.buildFailureMessage(),
                    retryCause
                )
            }
        }
    }

    private fun alertV2ContentParser(bytesReference: BytesReference): XContentParser {
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        return xcp
    }

    private fun scheduledJobContentParser(bytesReference: BytesReference): XContentParser {
        val xcp = XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
        return xcp
    }

    private fun areAlertV2IndicesPresent(): Boolean {
        return alertV2IndexInitialized && alertV2HistoryIndexInitialized
    }
}
