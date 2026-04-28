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
package com.embabel.agent.rag.neo.drivine.dialect

import org.drivine.manager.PersistenceManager
import org.slf4j.LoggerFactory

/**
 * Neptune Analytics RAG dialect implementation.
 *
 * Key limitations and differences from Neo4j:
 *
 * - **Vector indexes** are defined at graph creation time through the AWS API,
 *   not via Cypher. [createVectorIndexCypher] returns null (no-op).
 * - **Vector dimensions** are fixed at graph creation time and cannot be changed
 *   without recreating the graph.
 * - **Embedding storage** uses `neptune.algo.vectors.upsert()` procedure instead
 *   of a property set. Vector updates are **not ACID** — there is a window where
 *   just-ingested content is not yet searchable.
 * - **Fulltext search** is not supported. No native fulltext search capability.
 * - **Unique constraints** are not supported. Only node ID uniqueness is enforced
 *   by the engine; there are no property-level constraints.
 * - **Vector search** uses `neptune.algo.vectors.topKByEmbedding()`.
 *
 * @see <a href="https://docs.aws.amazon.com/neptune-analytics/latest/userguide/vectors.topK.byEmbedding.html">Neptune vectors.topKByEmbedding</a>
 * @see <a href="https://docs.aws.amazon.com/neptune-analytics/latest/userguide/vectors-upsert.html">Neptune vectors.upsert</a>
 */
class NeptuneAnalyticsRagDialect : RagDialect {

    private val logger = LoggerFactory.getLogger(NeptuneAnalyticsRagDialect::class.java)

    override val name = "Neptune Analytics"

    /**
     * Neptune Analytics vector indexes are defined at graph creation time through
     * the AWS API, not via Cypher. Skipping.
     */
    override fun createVectorIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        dimensions: Int,
        similarityFunction: String,
    ) {
        logger.info(
            "Neptune Analytics: vector indexes are defined at graph creation time via the AWS API. " +
                "Skipping Cypher-based vector index creation for '{}' on label '{}'.",
            name, label,
        )
    }

    /**
     * Neptune Analytics does not support fulltext search.
     */
    override fun createFullTextIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        properties: List<String>,
    ) {
        logger.info(
            "Neptune Analytics: fulltext search is not supported. " +
                "Skipping fulltext index creation for '{}' on label '{}'.",
            name, label,
        )
    }

    /**
     * Neptune Analytics does not support property-level unique constraints.
     * Only node ID uniqueness is enforced by the engine.
     */
    override fun createUniqueConstraint(
        persistenceManager: PersistenceManager,
        label: String,
        property: String,
    ) {
        logger.info(
            "Neptune Analytics: property-level unique constraints are not supported. " +
                "Only node ID uniqueness is enforced by the engine. " +
                "Skipping constraint creation for {}({}).",
            label, property,
        )
    }

    override fun chunkVectorSearchCypher(): String = """
        CALL neptune.algo.vectors.topKByEmbedding(${'$'}queryVector, {topK: ${'$'}topK})
        YIELD node AS chunk, score
          WHERE score >= ${'$'}similarityThreshold
          AND 'Chunk' IN labels(chunk)
        RETURN {
                 text:  chunk.text,
                 id:    chunk.id,
                 score: score
               } AS result
          ORDER BY result.score DESC""".trimIndent()

    /**
     * Neptune Analytics does not support fulltext search.
     *
     * @throws UnsupportedOperationException always
     */
    override fun chunkFullTextSearchCypher(): String? = null

    override fun entityVectorSearchCypher(): String = """
        CALL neptune.algo.vectors.topKByEmbedding(${'$'}queryVector, {topK: ${'$'}topK})
        YIELD node AS m, score
          WHERE score >= ${'$'}similarityThreshold
          AND any(label IN labels(m) WHERE label IN ${'$'}labels)
        RETURN {
                 properties:  properties(m),
                 name:        COALESCE(m.name, ''),
                 description: COALESCE(m.description, ''),
                 id:          COALESCE(m.id, ''),
                 labels:      labels(m),
                 score:       score
               } AS result
          ORDER BY result.score DESC""".trimIndent()

    /**
     * Neptune Analytics does not support fulltext search.
     */
    override fun entityFullTextSearchCypher(): String? = null

    /**
     * Neptune Analytics stores embeddings via the `neptune.algo.vectors.upsert()` procedure
     * rather than as a node property.
     *
     * **Note:** Vector updates in Neptune Analytics are not ACID. There is a window
     * where just-ingested content is not yet searchable.
     */
    override fun storeEmbeddingCypher(labels: String): String = """
        MERGE (n:$labels {id: ${'$'}id})
        SET n.embeddingModel = ${'$'}embeddingModel,
         n.embeddedAt = timestamp()
        FOREACH (x IN CASE WHEN coalesce(n.text, '') = ${'$'}embeddedText THEN [1] ELSE [] END |
            REMOVE n._text
        )
        FOREACH (x IN CASE WHEN coalesce(n.text, '') <> ${'$'}embeddedText THEN [1] ELSE [] END |
            SET n._text = ${'$'}embeddedText
        )
        WITH n
        CALL neptune.algo.vectors.upsert(n, ${'$'}embedding)
        YIELD node, success
        RETURN {nodesUpdated: CASE WHEN success THEN 1 ELSE 0 END }""".trimIndent()
}
