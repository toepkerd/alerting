/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.ExtractPPLIndicesAction
import org.opensearch.alerting.action.ExtractPPLIndicesRequest
import org.opensearch.alerting.action.ExtractPPLIndicesResponse
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.sql.ast.AbstractNodeVisitor
import org.opensearch.sql.ast.Node
import org.opensearch.sql.ast.expression.QualifiedName
import org.opensearch.sql.ast.tree.Relation
import org.opensearch.sql.ast.tree.UnresolvedPlan
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser
import org.opensearch.sql.ppl.parser.AstBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportExtractPPLIndicesAction::class.java)

class TransportExtractPPLIndicesAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<ExtractPPLIndicesRequest, ExtractPPLIndicesResponse>(
    ExtractPPLIndicesAction.NAME, transportService, actionFilters, ::ExtractPPLIndicesRequest
) {
    override fun doExecute(
        task: Task,
        request: ExtractPPLIndicesRequest,
        listener: ActionListener<ExtractPPLIndicesResponse>
    ) {
        try {
            val query = request.query
            log.info("PPL_INDEX_EXTRACT: Received query: $query")

            // Step 1: Parse PPL string into ANTLR parse tree
            val parser = PPLSyntaxParser()
            val parseTree = parser.parse(query)
            log.info("PPL_INDEX_EXTRACT: Parse tree created")

            // Step 2: Convert parse tree to UnresolvedPlan AST
            val astBuilder = AstBuilder(query)
            val plan: UnresolvedPlan = astBuilder.visit(parseTree)
            log.info("PPL_INDEX_EXTRACT: AST built, root type=${plan.javaClass.simpleName}")

            // Step 3: Walk the AST to extract Relation nodes
            val indices = mutableListOf<String>()
            extractIndices(plan, indices)
            log.info("PPL_INDEX_EXTRACT: Extracted indices=$indices")

            listener.onResponse(ExtractPPLIndicesResponse(indices))
        } catch (e: Exception) {
            log.error("PPL_INDEX_EXTRACT: Failed to extract indices", e)
            listener.onFailure(e)
        }
    }

    private fun extractIndices(node: Node, indices: MutableList<String>) {
        node.accept(
            object : AbstractNodeVisitor<Void?, Void?>() {
                override fun visitRelation(
                    relation: Relation,
                    context: Void?
                ): Void? {
                    val names = relation.qualifiedNames
                    log.info(
                        "PPL_INDEX_EXTRACT: Found Relation" +
                            " type=${relation.javaClass.simpleName}" +
                            " names=$names"
                    )
                    for (name: QualifiedName in names) {
                        val raw = name.parts.joinToString(".")
                        val cleaned = raw
                            .replace(".PPL_MAPPINGS_ODFE_SYS_TABLE", "")
                            .replace(".INFORMATION_SCHEMA_COLUMNS", "")
                        val split = cleaned.split(",")
                            .map { it.trim() }
                            .filter { it.isNotEmpty() }
                        indices.addAll(split)
                        log.info(
                            "PPL_INDEX_EXTRACT: Extracted=$split" +
                                " from raw=$raw"
                        )
                    }
                    return null
                }

                override fun visitChildren(
                    node: Node,
                    context: Void?
                ): Void? {
                    for (child in node.child) {
                        child.accept(this, context)
                    }
                    return null
                }
            },
            null
        )
    }
}
