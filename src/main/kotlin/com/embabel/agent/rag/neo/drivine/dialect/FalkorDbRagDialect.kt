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

import com.embabel.agent.rag.neo.drivine.model.FalkorDbIndexInfo
import org.drivine.manager.PersistenceManager
import org.drivine.query.QuerySpecification
import org.slf4j.LoggerFactory

/**
 * FalkorDB RAG dialect implementation.
 *
 * Key differences from Neo4j:
 * - Vector index: `CREATE VECTOR INDEX FOR (n:Label) ON (n.embedding) OPTIONS {dimension: N, similarityFunction: 'cosine'}`
 *   (no `IF NOT EXISTS`, no backtick-quoted config keys)
 * - Vector search: `CALL db.idx.vector.queryNodes(label, property, k, vecf32(vector))`
 *   (label and property as string arguments, `vecf32()` wrapper required)
 * - Fulltext index: `CALL db.idx.fulltext.createNodeIndex(label, property)`
 *   (procedure call, not DDL)
 * - Fulltext search: `CALL db.idx.fulltext.queryNodes(label, query)`
 *   (label as string argument instead of index name)
 * - Unique constraints: `GRAPH.CONSTRAINT CREATE` — a Redis command, not Cypher.
 *   Requires issuing through the FalkorDB driver directly; returns null here.
 *
 * @see <a href="https://docs.falkordb.com/cypher/indexing/vector-index.html">FalkorDB Vector Index</a>
 * @see <a href="https://docs.falkordb.com/cypher/indexing/fulltext-index.html">FalkorDB Fulltext Index</a>
 * @see <a href="https://docs.falkordb.com/commands/graph.constraint-create.html">FalkorDB Constraints</a>
 */
class FalkorDbRagDialect : RagDialect {

    private val logger = LoggerFactory.getLogger(FalkorDbRagDialect::class.java)

    override val name = "FalkorDB"

    override fun createVectorIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        dimensions: Int,
        similarityFunction: String,
    ) {
        if (indexExists(persistenceManager, label, "embedding", "VECTOR")) {
            logger.debug("Vector index on {}(embedding) already exists, skipping", label)
            return
        }
        persistenceManager.execute(QuerySpecification.withStatement(
            """
            CREATE VECTOR INDEX FOR (n:$label) ON (n.embedding)
            OPTIONS {dimension: $dimensions, similarityFunction: '$similarityFunction'}""".trimIndent()
        ))
    }

    override fun createFullTextIndex(
        persistenceManager: PersistenceManager,
        name: String,
        label: String,
        properties: List<String>,
    ) {
        // FalkorDB uses a procedure call per property rather than DDL.
        for (property in properties) {
            if (indexExists(persistenceManager, label, property, "FULLTEXT")) {
                logger.debug("Fulltext index on {}({}) already exists, skipping", label, property)
                continue
            }
            persistenceManager.execute(QuerySpecification.withStatement(
                "CALL db.idx.fulltext.createNodeIndex('$label', '$property')"
            ))
        }
    }

    /**
     * FalkorDB unique constraints use the Redis command `GRAPH.CONSTRAINT CREATE`,
     * which cannot be issued as Cypher through the persistence manager.
     * This must be handled through the FalkorDB Redis driver directly.
     */
    override fun createUniqueConstraint(
        persistenceManager: PersistenceManager,
        label: String,
        property: String,
    ) {
        logger.warn(
            "FalkorDB unique constraints require the Redis command " +
                "GRAPH.CONSTRAINT CREATE, which cannot be issued as Cypher. " +
                "Skipping constraint creation for {}({}). " +
                "Issue 'GRAPH.CONSTRAINT CREATE key UNIQUE NODE {} PROPERTIES 1 {}' " +
                "through the FalkorDB Redis driver directly.",
            label, property, label, property,
        )
    }

    /**
     * Check whether an index already exists on the given label/property/type
     * by querying `CALL db.indexes()`.
     *
     * FalkorDB's `db.indexes()` yields: label, properties, types, options,
     * language, stopwords, entitytype, status, info.
     *
     * @see <a href="https://docs.falkordb.com/cypher/procedures.html">FalkorDB Procedures</a>
     */
    private fun indexExists(persistenceManager: PersistenceManager, label: String, property: String, type: String): Boolean {
        val indexes = persistenceManager.query(
            QuerySpecification.withStatement(
                """CALL db.indexes()
                   YIELD label, properties, types
                   RETURN {label: label, properties: properties, types: types} AS idx"""
            ).transform(FalkorDbIndexInfo::class.java)
        )
        return indexes.any { it.label == label && it.hasIndex(property, type) }
    }

    override fun chunkVectorSearchCypher(): String = """
        CALL db.idx.vector.queryNodes(${'$'}chunkLabel, 'embedding', ${'$'}topK, vecf32(${'$'}queryVector))
        YIELD node AS chunk, score
          WHERE score >= ${'$'}similarityThreshold
        RETURN {
                 text:  chunk.text,
                 id:    chunk.id,
                 score: score
               } AS result
          ORDER BY result.score DESC""".trimIndent()

    override fun chunkFullTextSearchCypher(): String = """
        CALL db.idx.fulltext.queryNodes(${'$'}chunkLabel, ${'$'}searchText)
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
        CALL db.idx.vector.queryNodes(${'$'}entityNodeName, 'embedding', ${'$'}topK, vecf32(${'$'}queryVector))
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

    override fun entityFullTextSearchCypher(): String = """
        CALL db.idx.fulltext.queryNodes(${'$'}entityNodeName, ${'$'}searchText)
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
        SET n.embedding = ${'$'}embedding,
         n.embeddingModel = ${'$'}embeddingModel,
         n.embeddedAt = timestamp()
        FOREACH (x IN CASE WHEN coalesce(n.text, '') = ${'$'}embeddedText THEN [1] ELSE [] END |
            REMOVE n._text
        )
        FOREACH (x IN CASE WHEN coalesce(n.text, '') <> ${'$'}embeddedText THEN [1] ELSE [] END |
            SET n._text = ${'$'}embeddedText
        )
        RETURN {nodesUpdated: COUNT(n) }""".trimIndent()
}
