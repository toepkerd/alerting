/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.action.ExtractPPLIndicesAction
import org.opensearch.alerting.action.ExtractPPLIndicesRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(RestExtractPPLIndicesAction::class.java)

class RestExtractPPLIndicesAction : BaseRestHandler() {
    companion object {
        val ROUTE = "/_plugins/_alerting/ppl/_extract_indices"
    }

    override fun getName(): String = "extract_ppl_indices_action"

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.POST, ROUTE)
        )
    }

    override fun prepareRequest(
        request: RestRequest,
        client: NodeClient
    ): RestChannelConsumer {
        val parser = request.contentParser()
        var query = ""
        while (parser.nextToken() != null) {
            if (parser.currentName() == "query") {
                parser.nextToken()
                query = parser.text()
            }
        }
        log.info("PPL_INDEX_EXTRACT: REST received query=$query")
        return RestChannelConsumer { channel ->
            client.execute(
                ExtractPPLIndicesAction.INSTANCE,
                ExtractPPLIndicesRequest(query),
                RestToXContentListener(channel)
            )
        }
    }
}
