package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.rag.neo.drivine.test.TestAppContext
import com.embabel.common.ai.model.ModelProvider
import org.drivine.manager.PersistenceManager
import org.drivine.query.QuerySpecification
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.ai.mcp.client.common.autoconfigure.McpClientAutoConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
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
}
