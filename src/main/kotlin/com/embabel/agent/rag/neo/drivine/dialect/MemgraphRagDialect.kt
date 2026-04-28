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
import org.drivine.query.QuerySpecification
import org.slf4j.LoggerFactory

/**
 * Memgraph RAG dialect implementation.
 *
 * Memgraph is openCypher-native with Bolt protocol support, so most Cypher
 * works identically to Neo4j. Key differences:
 *
 * - Vector index: `CREATE VECTOR INDEX name ON :Label(prop) WITH CONFIG {dimension: N, capacity: M}`
 *   (uses `WITH CONFIG` instead of `OPTIONS`, no `IF NOT EXISTS`)
 * - Vector search: `CALL vector_search.search(indexName, k, vector) YIELD node, similarity`
 *   (yields `similarity` not `score`)
 * - Fulltext index: `CREATE TEXT INDEX name ON :Label(prop)`
 * - Fulltext search: `CALL text_search.search_all(indexName, query) YIELD node, score`
 * - Unique constraints: `CREATE CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE`
 *
 * @see <a href="https://memgraph.com/docs/querying/vector-search">Memgraph Vector Search</a>
 * @see <a href="https://memgraph.com/docs/querying/text-search">Memgraph Text Search</a>
 */
class MemgraphRagDialect : RagDialect {

    private val logger = LoggerFactory.getLogger(MemgraphRagDialect::class.java)

    override val name = "Memgraph"

    override fun createVectorIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        dimensions: Int,
        similarityFunction: String,
    ) {
        // Memgraph doesn't support IF NOT EXISTS for vector indexes.
        // Check existing indexes first.
        if (vectorIndexExists(persistenceManager, name)) {
            logger.debug("Vector index '{}' already exists, skipping", name)
            return
        }
        val metric = when (similarityFunction.lowercase()) {
            "cosine" -> "cos"
            "euclidean", "l2" -> "l2sq"
            "dot", "inner_product" -> "ip"
            else -> similarityFunction
        }
        persistenceManager.execute(QuerySpecification.withStatement(
            """
            CREATE VECTOR INDEX $name ON :$label($EMBEDDING_PROPERTY)
            WITH CONFIG {dimension: $dimensions, metric: "$metric", capacity: 10000}""".trimIndent()
        ))
    }

    override fun createFullTextIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        properties: List<String>,
    ) {
        val propsClause = properties.joinToString(", ")
        persistenceManager.execute(QuerySpecification.withStatement(
            "CREATE TEXT INDEX $name ON :$label($propsClause)"
        ))
    }

    override fun createUniqueConstraint(
        persistenceManager: PersistenceManager,
        label: String,
        property: String,
    ) {
        persistenceManager.execute(QuerySpecification.withStatement(
            "CREATE CONSTRAINT ON (n:$label) ASSERT n.$property IS UNIQUE"
        ))
    }

    override fun chunkVectorSearchCypher(): String = """
        CALL vector_search.search(${'$'}vectorIndex, ${'$'}topK, ${'$'}queryVector)
        YIELD node AS chunk, similarity AS score
          WHERE score >= ${'$'}similarityThreshold
        RETURN {
                 text:  chunk.text,
                 id:    chunk.id,
                 score: score
               } AS result
          ORDER BY result.score DESC""".trimIndent()

    override fun chunkFullTextSearchCypher(): String = """
        CALL text_search.search_all(${'$'}fulltextIndex, ${'$'}searchText)
        YIELD node AS chunk, score
        WITH collect({node: chunk, score: score}) AS results, max(score) AS maxScore
        UNWIND results AS result
        WITH result.node AS chunk,
             result.score / maxScore AS normalizedScore
          WHERE normalizedScore >= ${'$'}similarityThreshold
        RETURN {
                 text: chunk.text,
                 id:   chunk.id,
                 score: normalizedScore
               } AS result
          ORDER BY result.score DESC
          LIMIT ${'$'}topK""".trimIndent()

    override fun entityVectorSearchCypher(): String = """
        CALL vector_search.search(${'$'}index, ${'$'}topK, ${'$'}queryVector)
        YIELD node AS m, similarity AS score
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

    override fun entityFullTextSearchCypher(): String = """
        CALL text_search.search_all(${'$'}fulltextIndex, ${'$'}searchText)
        YIELD node AS m, score
        WHERE score IS NOT NULL AND any(label IN labels(m) WHERE label IN ${'$'}labels)
        WITH collect({node: m, score: score}) AS results, max(score) AS maxScore
          WHERE maxScore IS NOT NULL AND maxScore > 0
        UNWIND results AS result
        WITH result.node AS match,
             COALESCE(result.score / maxScore, 0.0) AS score,
             result.node.name AS name,
             result.node.description AS description,
             result.node.id AS id,
             labels(result.node) AS labels
          WHERE score >= ${'$'}similarityThreshold
        RETURN {
                 name:        COALESCE(name, ''),
                 description: COALESCE(description, ''),
                 id:          COALESCE(id, ''),
                 properties:  properties(match),
                 labels:      labels,
                 score:       score
               } AS result
          ORDER BY result.score DESC""".trimIndent()

    override fun storeEmbeddingCypher(labels: String): String = """
        MERGE (n:$labels {id: ${'$'}id})
        SET n.$EMBEDDING_PROPERTY = ${'$'}embedding,
         n.embeddingModel = ${'$'}embeddingModel,
         n.embeddedAt = timestamp()
        FOREACH (x IN CASE WHEN coalesce(n.text, '') = ${'$'}embeddedText THEN [1] ELSE [] END |
            REMOVE n._text
        )
        FOREACH (x IN CASE WHEN coalesce(n.text, '') <> ${'$'}embeddedText THEN [1] ELSE [] END |
            SET n._text = ${'$'}embeddedText
        )
        RETURN {nodesUpdated: COUNT(n) }""".trimIndent()

    private fun vectorIndexExists(persistenceManager: PersistenceManager, name: String): Boolean {
        return try {
            @Suppress("UNCHECKED_CAST")
            val indexes = persistenceManager.query(
                QuerySpecification
                    .withStatement("CALL vector_search.show_index_info() YIELD * RETURN *")
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>
            indexes.any { it["index_name"]?.toString() == name }
        } catch (e: Exception) {
            logger.debug("Could not query vector indexes: {}", e.message)
            false
        }
    }

    companion object {
        private const val EMBEDDING_PROPERTY = "embedding"
    }
}
