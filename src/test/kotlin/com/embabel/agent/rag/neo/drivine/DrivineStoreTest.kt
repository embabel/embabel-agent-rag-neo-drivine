package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.rag.model.Chunk
import com.embabel.agent.rag.neo.drivine.test.TestAppContext
import com.embabel.common.ai.model.ModelProvider
import org.drivine.manager.PersistenceManager
import org.drivine.query.QuerySpecification
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.ai.mcp.client.common.autoconfigure.McpClientAutoConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.util.UUID

@SpringBootTest(classes = [TestAppContext::class])
@ImportAutoConfiguration(exclude = [McpClientAutoConfiguration::class])
class DrivineStoreTest {
    @MockitoBean
    lateinit var modelProvider: ModelProvider

    @Autowired
    lateinit var drivineStore: DrivineStore

    @Autowired
    @Qualifier("neo")
    lateinit var persistenceManager: PersistenceManager

    private val testNodeIds = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        testNodeIds.clear()
    }

    @AfterEach
    fun tearDown() {
        if (testNodeIds.isNotEmpty()) {
            val deleteStatement = "MATCH (n) WHERE n.id IN \$ids DETACH DELETE n"
            persistenceManager.execute(
                QuerySpecification.withStatement(deleteStatement)
                    .bind(mapOf("ids" to testNodeIds))
            )
        }
    }

    private fun createTestNode(labels: List<String>, uri: String): String {
        val id = UUID.randomUUID().toString()
        val labelsString = labels.joinToString(":")
        val createStatement = """
            CREATE (n:$labelsString {
                id: ${'$'}id,
                uri: ${'$'}uri,
                text: 'Test content',
                parentId: null,
                ingestionTimestamp: datetime()
            })
            RETURN n.id as id
        """.trimIndent()
        persistenceManager.execute(
            QuerySpecification.withStatement(createStatement)
                .bind(mapOf("id" to id, "uri" to uri))
        )
        testNodeIds.add(id)
        return id
    }

    @Test
    fun `should return info`() {
        val info = drivineStore.info()
        println("Info: $info")
        assertNotNull(info)
    }

    @Test
    fun `should find content root by uri for Document node`() {
        val uri = "test://document-node-${UUID.randomUUID()}"
        createTestNode(listOf("ContentElement", "Document"), uri)

        val result = drivineStore.findContentRootByUri(uri)

        println("Found Document content root: $result")
        assertNotNull(result, "Expected to find a ContentRoot with Document label and URI: $uri")
    }

    @Test
    fun `should find content root by uri for Document with Section labels`() {
        val uri = "test://document-section-${UUID.randomUUID()}"
        createTestNode(listOf("ContentElement", "Document", "Section", "ContainerSection"), uri)

        val result = drivineStore.findContentRootByUri(uri)

        println("Found Document+Section content root: $result")
        assertNotNull(result, "Expected to find a ContentRoot with Document+Section labels and URI: $uri")
    }

    @Test
    fun `should find content root by uri for ContentRoot label without Document`() {
        val uri = "test://content-root-only-${UUID.randomUUID()}"
        createTestNode(listOf("ContentElement", "ContentRoot"), uri)

        val result = drivineStore.findContentRootByUri(uri)

        println("Found ContentRoot (no Document label): $result")
        assertNotNull(result, "Expected to find a ContentRoot with ContentRoot label (no Document) and URI: $uri")
    }

    @Test
    fun `should return null for non-existent uri`() {
        val uri = "test://non-existent-${UUID.randomUUID()}"

        val result = drivineStore.findContentRootByUri(uri)

        assertNull(result, "Expected null for non-existent URI")
    }

    @Test
    fun `should not find Chunk node as content root`() {
        val uri = "test://chunk-node-${UUID.randomUUID()}"
        createTestNode(listOf("ContentElement", "Chunk"), uri)

        val result = drivineStore.findContentRootByUri(uri)

        assertNull(result, "Chunk nodes should not be returned as ContentRoot")
    }

    @Nested
    inner class ChunkMetadataPersistence {

        @Test
        fun `should persist chunk metadata`() {
            val chunkId = UUID.randomUUID().toString()
            val parentId = UUID.randomUUID().toString()
            val metadata = mapOf(
                "source" to "test-source",
                "section_title" to "Test Section",
                "page_number" to 42
            )
            val chunk = Chunk(
                id = chunkId,
                text = "This is test chunk content",
                parentId = parentId,
                metadata = metadata
            )
            testNodeIds.add(chunkId)

            drivineStore.save(chunk)

            val retrieved = drivineStore.findById(chunkId)
            println("Saved chunk: $chunk")
            println("Retrieved chunk: $retrieved")

            assertNotNull(retrieved, "Chunk should be retrievable after save")
            require(retrieved is Chunk) { "Retrieved element should be a Chunk" }
            assertEquals(chunkId, retrieved.id)
            assertEquals("This is test chunk content", retrieved.text)
            assertEquals(parentId, retrieved.parentId)

            // Verify metadata is preserved
            assertEquals("test-source", retrieved.metadata["source"], "source metadata should be preserved")
            assertEquals("Test Section", retrieved.metadata["section_title"], "section_title metadata should be preserved")
            // Neo4j returns integers as Long
            assertEquals(42L, retrieved.metadata["page_number"], "page_number metadata should be preserved")
        }

        @Test
        fun `should persist chunk with empty metadata`() {
            val chunkId = UUID.randomUUID().toString()
            val parentId = UUID.randomUUID().toString()
            val chunk = Chunk(
                id = chunkId,
                text = "Chunk with no metadata",
                parentId = parentId,
                metadata = emptyMap()
            )
            testNodeIds.add(chunkId)

            drivineStore.save(chunk)

            val retrieved = drivineStore.findById(chunkId)
            assertNotNull(retrieved, "Chunk should be retrievable after save")
            require(retrieved is Chunk) { "Retrieved element should be a Chunk" }
            assertEquals(chunkId, retrieved.id)
        }

        @Test
        fun `should retrieve chunks by id with metadata preserved`() {
            val chunkId = UUID.randomUUID().toString()
            val parentId = UUID.randomUUID().toString()
            val metadata = mapOf(
                "source" to "bulk-test-source",
                "custom_field" to "custom_value"
            )
            val chunk = Chunk(
                id = chunkId,
                text = "Bulk retrieval test chunk",
                parentId = parentId,
                metadata = metadata
            )
            testNodeIds.add(chunkId)

            drivineStore.save(chunk)

            val retrieved = drivineStore.findAllChunksById(listOf(chunkId)).toList()
            println("Retrieved chunks: $retrieved")

            assertEquals(1, retrieved.size, "Should retrieve exactly one chunk")
            val retrievedChunk = retrieved.first()
            assertEquals(chunkId, retrievedChunk.id)
            assertEquals("bulk-test-source", retrievedChunk.metadata["source"], "source metadata should be preserved in bulk retrieval")
            assertEquals("custom_value", retrievedChunk.metadata["custom_field"], "custom_field metadata should be preserved in bulk retrieval")
        }
    }
}
