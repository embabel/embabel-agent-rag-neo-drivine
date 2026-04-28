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
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.drivine.connection.DatabaseType
import org.drivine.manager.PersistenceManager
import org.drivine.query.QuerySpecification
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RagDialectTest {

    @Test
    fun `forDatabaseType resolves Neo4j`() {
        val dialect = RagDialect.forDatabaseType(DatabaseType.NEO4J)
        assertTrue(dialect is Neo4jRagDialect)
    }

    @Test
    fun `forDatabaseType resolves FalkorDB`() {
        val dialect = RagDialect.forDatabaseType(DatabaseType.FALKORDB)
        assertTrue(dialect is FalkorDbRagDialect)
    }

    @Test
    fun `forDatabaseType resolves Neptune`() {
        val dialect = RagDialect.forDatabaseType(DatabaseType.NEPTUNE)
        assertTrue(dialect is NeptuneAnalyticsRagDialect)
    }

    @Test
    fun `forDatabaseType rejects unsupported types`() {
        assertThrows<IllegalArgumentException> {
            RagDialect.forDatabaseType(DatabaseType.POSTGRES)
        }
    }

    @Test
    fun `forDatabaseType resolves Memgraph`() {
        val dialect = RagDialect.forDatabaseType(DatabaseType.MEMGRAPH)
        assertTrue(dialect is MemgraphRagDialect)
    }

    @Nested
    inner class Neo4j {
        private val dialect = Neo4jRagDialect()
        private val pm = mockk<PersistenceManager>(relaxed = true)

        @Test
        fun `creates vector index with IF NOT EXISTS`() {
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createVectorIndex(pm, "idx", "Chunk", 384, "cosine")

            verify(exactly = 1) { pm.execute(any()) }
            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE VECTOR INDEX `idx` IF NOT EXISTS"))
            assertTrue(cypher.contains("`vector.dimensions`: 384"))
            assertTrue(cypher.contains("`vector.similarity_function`: 'cosine'"))
        }

        @Test
        fun `creates fulltext index`() {
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createFullTextIndex(pm, "idx", "Chunk", listOf("text"))

            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE FULLTEXT INDEX `idx` IF NOT EXISTS"))
            assertTrue(cypher.contains("ON EACH [n.text]"))
        }

        @Test
        fun `creates unique constraint`() {
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createUniqueConstraint(pm, "Entity", "id")

            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE CONSTRAINT `entity_id_unique` IF NOT EXISTS"))
            assertTrue(cypher.contains("REQUIRE n.id IS UNIQUE"))
        }

        @Test
        fun `chunk vector search uses db_index_vector_queryNodes`() {
            val cypher = dialect.chunkVectorSearchCypher()
            assertTrue(cypher.contains("db.index.vector.queryNodes"))
            assertTrue(cypher.contains("\$vectorIndex"))
        }

        @Test
        fun `chunk fulltext search is supported`() {
            assertNotNull(dialect.chunkFullTextSearchCypher())
        }

        @Test
        fun `entity fulltext search is supported`() {
            assertNotNull(dialect.entityFullTextSearchCypher())
        }

        @Test
        fun `store embedding uses property set`() {
            val cypher = dialect.storeEmbeddingCypher("Chunk:ContentElement")
            assertTrue(cypher.contains("SET n.embedding"))
            assertTrue(cypher.contains("Chunk:ContentElement"))
            assertTrue(cypher.contains("\$embeddedText"))
            assertTrue(cypher.contains("REMOVE n._text"))
            assertTrue(cypher.contains("SET n._text"))
        }
    }

    @Nested
    inner class FalkorDB {
        private val dialect = FalkorDbRagDialect()

        private fun pmWithNoIndexes(): PersistenceManager {
            val pm = mockk<PersistenceManager>(relaxed = true)
            every { pm.query<Any>(any()) } returns emptyList()
            return pm
        }

        private fun pmWithExistingIndex(label: String, property: String, type: String): PersistenceManager {
            val pm = mockk<PersistenceManager>(relaxed = true)
            val indexInfo = FalkorDbIndexInfo(
                label = label,
                properties = listOf(property),
                types = mapOf(property to listOf(type)),
            )
            every { pm.query<Any>(any()) } returns listOf(indexInfo)
            return pm
        }

        @Test
        fun `creates vector index when none exists`() {
            val pm = pmWithNoIndexes()
            dialect.createVectorIndex(pm, "idx", "Chunk", 384, "cosine")
            verify(exactly = 1) { pm.execute(match { it.statement?.text?.contains("CREATE VECTOR INDEX") == true }) }
        }

        @Test
        fun `skips vector index when already exists`() {
            val pm = pmWithExistingIndex("Chunk", "embedding", "VECTOR")
            dialect.createVectorIndex(pm, "idx", "Chunk", 384, "cosine")
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `creates fulltext index when none exists`() {
            val pm = pmWithNoIndexes()
            dialect.createFullTextIndex(pm, "idx", "Chunk", listOf("text"))
            verify(exactly = 1) { pm.execute(match { it.statement?.text?.contains("createNodeIndex") == true }) }
        }

        @Test
        fun `skips fulltext index when already exists`() {
            val pm = pmWithExistingIndex("Chunk", "text", "FULLTEXT")
            dialect.createFullTextIndex(pm, "idx", "Chunk", listOf("text"))
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `creates fulltext index for each property`() {
            val pm = pmWithNoIndexes()
            dialect.createFullTextIndex(pm, "idx", "Entity", listOf("name", "description"))
            verify(exactly = 2) { pm.execute(any()) }
        }

        @Test
        fun `unique constraint logs warning and does not execute`() {
            val pm = mockk<PersistenceManager>(relaxed = true)
            dialect.createUniqueConstraint(pm, "Entity", "id")
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `chunk vector search uses db_idx_vector_queryNodes with vecf32`() {
            val cypher = dialect.chunkVectorSearchCypher()
            assertTrue(cypher.contains("db.idx.vector.queryNodes"))
            assertTrue(cypher.contains("vecf32"))
            assertTrue(cypher.contains("\$chunkLabel"))
        }

        @Test
        fun `chunk fulltext search uses db_idx_fulltext_queryNodes`() {
            val cypher = dialect.chunkFullTextSearchCypher()
            assertNotNull(cypher)
            assertTrue(cypher!!.contains("db.idx.fulltext.queryNodes"))
            assertTrue(cypher.contains("\$chunkLabel"))
        }

        @Test
        fun `entity vector search uses entityNodeName param`() {
            val cypher = dialect.entityVectorSearchCypher()
            assertTrue(cypher.contains("\$entityNodeName"))
            assertTrue(cypher.contains("vecf32"))
        }

        @Test
        fun `store embedding uses property set`() {
            val cypher = dialect.storeEmbeddingCypher("Chunk:ContentElement")
            assertTrue(cypher.contains("SET n.embedding"))
            assertTrue(cypher.contains("\$embeddedText"))
            assertTrue(cypher.contains("REMOVE n._text"))
            assertTrue(cypher.contains("SET n._text"))
        }
    }

    @Nested
    inner class NeptuneAnalytics {
        private val dialect = NeptuneAnalyticsRagDialect()
        private val pm = mockk<PersistenceManager>(relaxed = true)

        @Test
        fun `vector index creation is a no-op`() {
            dialect.createVectorIndex(pm, "idx", "Chunk", 384, "cosine")
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `fulltext index creation is a no-op`() {
            dialect.createFullTextIndex(pm, "idx", "Chunk", listOf("text"))
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `unique constraint is a no-op`() {
            dialect.createUniqueConstraint(pm, "Entity", "id")
            verify(exactly = 0) { pm.execute(any()) }
        }

        @Test
        fun `chunk vector search uses neptune_algo_vectors`() {
            val cypher = dialect.chunkVectorSearchCypher()
            assertTrue(cypher.contains("neptune.algo.vectors.topKByEmbedding"))
        }

        @Test
        fun `chunk fulltext search returns null`() {
            assertNull(dialect.chunkFullTextSearchCypher())
        }

        @Test
        fun `entity fulltext search returns null`() {
            assertNull(dialect.entityFullTextSearchCypher())
        }

        @Test
        fun `store embedding uses vectors_upsert procedure`() {
            val cypher = dialect.storeEmbeddingCypher("Chunk:ContentElement")
            assertTrue(cypher.contains("neptune.algo.vectors.upsert"))
            assertTrue(!cypher.contains("SET n.embedding ="), "Neptune should not set embedding as a property")
            assertTrue(cypher.contains("\$embeddedText"))
            assertTrue(cypher.contains("REMOVE n._text"))
            assertTrue(cypher.contains("SET n._text"))
        }
    }

    @Nested
    inner class Memgraph {
        private val dialect = MemgraphRagDialect()

        @Test
        fun `forName resolves Memgraph`() {
            assertTrue(RagDialect.forName("memgraph") is MemgraphRagDialect)
            assertTrue(RagDialect.forName("MEMGRAPH") is MemgraphRagDialect)
        }

        @Test
        fun `creates vector index with cosine mapped to cos`() {
            val pm = mockk<PersistenceManager>(relaxed = true)
            every { pm.query<Any>(any()) } returns emptyList()
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createVectorIndex(pm, "idx", "Chunk", 384, "cosine")

            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE VECTOR INDEX idx ON :Chunk(embedding)"))
            assertTrue(cypher.contains("dimension: 384"))
            assertTrue(cypher.contains("metric: \"cos\""))
        }

        @Test
        fun `creates fulltext index`() {
            val pm = mockk<PersistenceManager>(relaxed = true)
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createFullTextIndex(pm, "idx", "Chunk", listOf("text"))

            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE TEXT INDEX idx ON :Chunk(text)"))
        }

        @Test
        fun `creates unique constraint`() {
            val pm = mockk<PersistenceManager>(relaxed = true)
            val spec = slot<QuerySpecification<*>>()
            every { pm.execute(capture(spec)) } returns Unit

            dialect.createUniqueConstraint(pm, "Entity", "id")

            val cypher = spec.captured.statement!!.text
            assertTrue(cypher.contains("CREATE CONSTRAINT"))
            assertTrue(cypher.contains("n.id IS UNIQUE"))
        }

        @Test
        fun `chunk vector search uses vector_search_search`() {
            val cypher = dialect.chunkVectorSearchCypher()
            assertTrue(cypher.contains("vector_search.search"))
            assertTrue(cypher.contains("similarity AS score"))
        }

        @Test
        fun `chunk fulltext search uses text_search_search_all`() {
            val cypher = dialect.chunkFullTextSearchCypher()
            assertNotNull(cypher)
            assertTrue(cypher!!.contains("text_search.search_all"))
        }

        @Test
        fun `entity vector search uses vector_search_search`() {
            val cypher = dialect.entityVectorSearchCypher()
            assertTrue(cypher.contains("vector_search.search"))
            assertTrue(cypher.contains("similarity AS score"))
        }

        @Test
        fun `entity fulltext search is supported`() {
            assertNotNull(dialect.entityFullTextSearchCypher())
        }

        @Test
        fun `store embedding uses property set`() {
            val cypher = dialect.storeEmbeddingCypher("Chunk:ContentElement")
            assertTrue(cypher.contains("SET n.embedding"))
            assertTrue(cypher.contains("Chunk:ContentElement"))
            assertTrue(cypher.contains("\$embeddedText"))
            assertTrue(cypher.contains("REMOVE n._text"))
            assertTrue(cypher.contains("SET n._text"))
        }
    }
}
