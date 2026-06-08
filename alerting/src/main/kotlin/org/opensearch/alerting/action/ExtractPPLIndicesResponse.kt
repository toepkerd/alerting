/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class ExtractPPLIndicesResponse : ActionResponse, ToXContentObject {
    val indices: List<String>

    constructor(indices: List<String>) : super() {
        this.indices = indices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        indices = sin.readStringList()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("indices", indices)
        builder.endObject()
        return builder
    }
}
