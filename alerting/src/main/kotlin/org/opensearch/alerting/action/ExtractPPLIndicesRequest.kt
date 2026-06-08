/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class ExtractPPLIndicesRequest : ActionRequest {
    val query: String

    constructor(query: String) : super() {
        this.query = query
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        query = sin.readString()
    }

    override fun validate(): ActionRequestValidationException? = null

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(query)
    }
}
