/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

class ExtractPPLIndicesAction private constructor() : ActionType<ExtractPPLIndicesResponse>(NAME, ::ExtractPPLIndicesResponse) {
    companion object {
        val INSTANCE = ExtractPPLIndicesAction()
        const val NAME = "cluster:admin/opendistro/alerting/ppl/extract_indices"
    }
}
