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

import com.embabel.agent.rag.neo.drivine.NeoRagServiceProperties
import org.drivine.autoconfigure.DrivinePropertiesConfiguration
import org.drivine.autoconfigure.DrivineTestConfiguration
import org.drivine.autoconfigure.EnableDrivineTestConfig
import org.drivine.manager.PersistenceManager
import org.drivine.manager.PersistenceManagerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.EnableAspectJAutoProxy
import org.springframework.context.annotation.FilterType

/**
 * Test configuration for Drivine-based tests.
 *
 * Uses Drivine's DrivineTestConfiguration which automatically handles:
 * - Testcontainers (default): starts Neo4j container automatically
 * - Local Neo4j: set USE_LOCAL_NEO4J=true env var or test.neo4j.use-local=true property
 *
 * Datasource properties are configured in application.yml under database.datasources.neo
 */
@Configuration
@EnableDrivineTestConfig
@ComponentScan(
    basePackages = ["org.drivine", "com.embabel.agent.rag.neo.drivine"],
    excludeFilters = [ComponentScan.Filter(
        type = FilterType.ASSIGNABLE_TYPE,
        classes = [
            DrivinePropertiesConfiguration::class,
            DrivineTestConfiguration::class,
            RagTestShellApplication::class,
            RagShellCommands::class,
            RagChatActions::class
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
}