/*
 * Copyright 2024-2026 Embabel Pty Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.embabel.agent.rag.neo.drivine.mappers

import com.embabel.agent.rag.model.Chunk
import com.embabel.agent.rag.model.ContentElement
import com.embabel.agent.rag.model.LeafSection
import com.embabel.agent.rag.model.MaterializedDocument
import org.drivine.mapper.RowMapper
import java.time.Instant
import java.time.ZonedDateTime

internal class DefaultContentElementRowMapper : RowMapper<ContentElement> {

    /**
     * Core properties that are NOT metadata.
     * Everything else in the node properties is considered metadata.
     */
    private val CORE_PROPERTIES = setOf(
        "id",
        "title",
        "text",
        "parentId",
        "uri",
        "ingestionTimestamp",
        "lastModifiedDate",
        "embedding",
        "embeddingModel",
        "embeddedAt"
    )

    override fun map(row: Map<String, *>): ContentElement {
        val labels = row["labels"] as? List<*> ?: error("Must have labels")
        val allProperties = row["properties"] as? Map<*, *> ?: emptyMap<String, Any>()
        val metadata = extractMetadata(allProperties)

        if (labels.contains("Chunk"))
            return Chunk.Companion(
                id = row["id"] as String,
                text = row["text"] as String,
                parentId = row["parentId"] as String,
                metadata = metadata,
            )
        if (labels.contains("LeafSection"))
            return LeafSection(
                id = row["id"] as String,
                title = allProperties["title"] as? String ?: "",
                text = row["text"] as? String ?: "",
                parentId = row["parentId"] as? String,
                uri = row["uri"] as? String,
                metadata = metadata,
            )
        if (labels.contains("Document") || labels.contains("ContentRoot")) {
            val ingestionDate = when (val rawDate = row["ingestionDate"]) {
                is Instant -> rawDate
                is ZonedDateTime -> rawDate.toInstant()
                is Long -> Instant.ofEpochMilli(rawDate)
                is String -> Instant.parse(rawDate)
                null -> Instant.now()
                else -> Instant.now()
            }
            val title = allProperties["title"] as? String ?: row["id"] as String
            return MaterializedDocument(
                id = row["id"] as String,
                title = title,
                children = emptyList(),
                metadata = metadata,
                uri = row["uri"] as String,
                ingestionTimestamp = ingestionDate,
            )
        }
        throw RuntimeException("Don't know how to map: $labels")
    }

    /**
     * Extracts metadata from node properties by filtering out core properties.
     */
    private fun extractMetadata(allProperties: Map<*, *>): Map<String, Any> {
        return allProperties
            .filterKeys { key -> key is String && key !in CORE_PROPERTIES }
            .mapKeys { it.key as String }
            .mapValues { it.value as Any }
    }
}
