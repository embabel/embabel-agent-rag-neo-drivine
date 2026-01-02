/*
 * Copyright 2024-2025 Embabel Software, Inc.
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
package com.embabel.agent.rag.neo.drivine.test

import com.embabel.agent.rag.neo.drivine.DrivineCypherSearch
import com.embabel.agent.rag.neo.drivine.DrivineStore
import com.embabel.agent.rag.neo.drivine.NeoRagServiceProperties
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.ai.model.SpringEmbeddingService
import com.embabel.common.util.generateRandomFloatArray
import org.drivine.autoconfigure.EnableDrivine
import org.drivine.autoconfigure.EnableDrivineTestConfig
import org.drivine.manager.PersistenceManager
import org.drivine.manager.PersistenceManagerFactory
import org.springframework.ai.document.Document
import org.springframework.ai.embedding.Embedding
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.ai.embedding.EmbeddingRequest
import org.springframework.ai.embedding.EmbeddingResponse
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.EnableAspectJAutoProxy
import org.springframework.context.annotation.FilterType
import org.springframework.transaction.PlatformTransactionManager
import java.util.LinkedList

/**
 * Test configuration for Drivine-based tests.
 *
 * Uses @EnableDrivine and @EnableDrivineTestConfig which handle:
 * - Testcontainers (default): starts Neo4j container automatically
 * - Local Neo4j: set USE_LOCAL_NEO4J=true env var or test.neo4j.use-local=true property
 *
 * Note: Do NOT use @EnableDrivinePropertiesConfig with @EnableDrivineTestConfig - the test
 * config provides its own DataSourceMap with testcontainer support.
 *
 * Datasource properties are configured in application.yml under database.datasources.neo
 */
@Configuration
@EnableDrivine
@EnableDrivineTestConfig
@ComponentScan(
    basePackages = ["com.embabel.agent.rag.neo.drivine"],
    excludeFilters = [ComponentScan.Filter(
        type = FilterType.ASSIGNABLE_TYPE,
        classes = [
            RagTestShellApplication::class,
            RagShellCommands::class,
            RagChatActions::class,
            DrivineCypherSearch::class,
            DrivineStore::class
        ]
    )]
)
@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableConfigurationProperties(NeoRagServiceProperties::class)
class TestAppContext {

    @Bean("neo")
    fun persistenceManager(factory: PersistenceManagerFactory): PersistenceManager {
        return factory.get("neo")
    }

    @Bean
    fun drivineCypherSearch(persistenceManager: PersistenceManager): DrivineCypherSearch {
        return DrivineCypherSearch(persistenceManager)
    }

    @Bean
    fun drivineStore(
        persistenceManager: PersistenceManager,
        properties: NeoRagServiceProperties,
        transactionManager: PlatformTransactionManager,
        cypherSearch: DrivineCypherSearch
    ): DrivineStore {
        return DrivineStore(
            persistenceManager = persistenceManager,
            properties = properties,
            platformTransactionManager = transactionManager,
            cypherSearch = cypherSearch,
            embeddingService = SpringEmbeddingService("fake", "embabel", FakeEmbeddingModel())
        )
    }
}

data class FakeEmbeddingModel(
    val dimensions: Int = 1536,
) : EmbeddingModel {

    override fun embed(document: Document): FloatArray {
        return generateRandomFloatArray(dimensions)
    }

    override fun embed(texts: List<String>): MutableList<FloatArray> {
        return texts.map { generateRandomFloatArray(dimensions) }.toMutableList()
    }

    override fun call(request: EmbeddingRequest): EmbeddingResponse {
        val output = LinkedList<Embedding>()
        for (i in request.instructions.indices) {
            output.add(Embedding(generateRandomFloatArray(dimensions), i))
        }
        return EmbeddingResponse(output)
    }
}

