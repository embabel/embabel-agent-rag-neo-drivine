package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.rag.neo.drivine.test.TestAppContext
import com.embabel.common.ai.model.ModelProvider
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.ai.mcp.client.common.autoconfigure.McpClientAutoConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean

@SpringBootTest(classes = [TestAppContext::class])
@ImportAutoConfiguration(exclude = [McpClientAutoConfiguration::class])
class DrivineStoreTest {
    @MockitoBean
    lateinit var modelProvider: ModelProvider

    @Autowired
    lateinit var drivineStore: DrivineStore

    @Test
    fun `should return info`() {
        val info = drivineStore.info()
        println("Info: $info")
        assertNotNull(info)
    }
}
