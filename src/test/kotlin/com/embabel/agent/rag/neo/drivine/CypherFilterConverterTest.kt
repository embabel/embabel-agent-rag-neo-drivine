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
package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.rag.filter.PropertyFilter
import com.embabel.agent.rag.filter.PropertyFilter.Companion.contains
import com.embabel.agent.rag.filter.PropertyFilter.Companion.eq
import com.embabel.agent.rag.filter.PropertyFilter.Companion.gt
import com.embabel.agent.rag.filter.PropertyFilter.Companion.gte
import com.embabel.agent.rag.filter.PropertyFilter.Companion.lt
import com.embabel.agent.rag.filter.PropertyFilter.Companion.lte
import com.embabel.agent.rag.filter.PropertyFilter.Companion.ne
import com.embabel.agent.rag.filter.PropertyFilter.Companion.nin
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class CypherFilterConverterTest {

    private val converter = CypherFilterConverter(nodeAlias = "e")

    @Nested
    inner class NullFilterTests {

        @Test
        fun `null filter returns empty result`() {
            val result = converter.convert(null)

            assertTrue(result.isEmpty())
            assertEquals("", result.whereClause)
            assertTrue(result.parameters.isEmpty())
        }
    }

    @Nested
    inner class ComparisonOperatorTests {

        @Test
        fun `Eq converts to equality`() {
            val result = converter.convert(eq("owner", "alice"))

            assertEquals("e.owner = \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to "alice"), result.parameters)
        }

        @Test
        fun `Ne converts to inequality`() {
            val result = converter.convert(ne("status", "deleted"))

            assertEquals("e.status <> \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to "deleted"), result.parameters)
        }

        @Test
        fun `Gt converts to greater than`() {
            val result = converter.convert(gt("score", 0.5))

            assertEquals("e.score > \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to 0.5), result.parameters)
        }

        @Test
        fun `Gte converts to greater than or equal`() {
            val result = converter.convert(gte("count", 10))

            assertEquals("e.count >= \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to 10), result.parameters)
        }

        @Test
        fun `Lt converts to less than`() {
            val result = converter.convert(lt("priority", 5))

            assertEquals("e.priority < \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to 5), result.parameters)
        }

        @Test
        fun `Lte converts to less than or equal`() {
            val result = converter.convert(lte("level", 3))

            assertEquals("e.level <= \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to 3), result.parameters)
        }
    }

    @Nested
    inner class ListOperatorTests {

        @Test
        fun `In converts to IN list`() {
            val result = converter.convert(PropertyFilter.`in`("status", "active", "pending"))

            assertEquals("e.status IN \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to listOf("active", "pending")), result.parameters)
        }

        @Test
        fun `Nin converts to NOT IN list`() {
            val result = converter.convert(nin("status", "deleted", "archived"))

            assertEquals("NOT e.status IN \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to listOf("deleted", "archived")), result.parameters)
        }
    }

    @Nested
    inner class StringOperatorTests {

        @Test
        fun `Contains converts to CONTAINS`() {
            val result = converter.convert(contains("description", "machine learning"))

            assertEquals("e.description CONTAINS \$_filter_0", result.whereClause)
            assertEquals(mapOf("_filter_0" to "machine learning"), result.parameters)
        }
    }

    @Nested
    inner class LogicalOperatorTests {

        @Test
        fun `And converts to AND clause`() {
            val result = converter.convert(eq("owner", "alice") and eq("status", "active"))

            assertEquals("(e.owner = \$_filter_0) AND (e.status = \$_filter_1)", result.whereClause)
            assertEquals(
                mapOf("_filter_0" to "alice", "_filter_1" to "active"),
                result.parameters
            )
        }

        @Test
        fun `Or converts to OR clause`() {
            val result = converter.convert(eq("owner", "alice") or eq("owner", "bob"))

            assertEquals("(e.owner = \$_filter_0) OR (e.owner = \$_filter_1)", result.whereClause)
            assertEquals(
                mapOf("_filter_0" to "alice", "_filter_1" to "bob"),
                result.parameters
            )
        }

        @Test
        fun `Not converts to NOT clause`() {
            val result = converter.convert(!eq("status", "deleted"))

            assertEquals("NOT (e.status = \$_filter_0)", result.whereClause)
            assertEquals(mapOf("_filter_0" to "deleted"), result.parameters)
        }

        @Test
        fun `single filter And returns unwrapped`() {
            val result = converter.convert(PropertyFilter.And(eq("owner", "alice")))

            assertEquals("e.owner = \$_filter_0", result.whereClause)
        }

        @Test
        fun `single filter Or returns unwrapped`() {
            val result = converter.convert(PropertyFilter.Or(eq("owner", "alice")))

            assertEquals("e.owner = \$_filter_0", result.whereClause)
        }
    }

    @Nested
    inner class ComplexExpressionTests {

        @Test
        fun `complex nested expression`() {
            // (owner == "alice" AND status == "active") OR role == "admin"
            val filter = (eq("owner", "alice") and eq("status", "active")) or eq("role", "admin")

            val result = converter.convert(filter)

            assertEquals(
                "((e.owner = \$_filter_0) AND (e.status = \$_filter_1)) OR (e.role = \$_filter_2)",
                result.whereClause
            )
            assertEquals(
                mapOf(
                    "_filter_0" to "alice",
                    "_filter_1" to "active",
                    "_filter_2" to "admin"
                ),
                result.parameters
            )
        }

        @Test
        fun `And with Not`() {
            // owner == "alice" AND NOT status == "deleted"
            val filter = eq("owner", "alice") and !eq("status", "deleted")

            val result = converter.convert(filter)

            assertEquals(
                "(e.owner = \$_filter_0) AND (NOT (e.status = \$_filter_1))",
                result.whereClause
            )
            assertEquals(
                mapOf("_filter_0" to "alice", "_filter_1" to "deleted"),
                result.parameters
            )
        }

        @Test
        fun `double negation`() {
            val filter = !!eq("owner", "alice")

            val result = converter.convert(filter)

            assertEquals("NOT (NOT (e.owner = \$_filter_0))", result.whereClause)
            assertEquals(mapOf("_filter_0" to "alice"), result.parameters)
        }

        @Test
        fun `three-way And`() {
            val filter = PropertyFilter.and(
                eq("owner", "alice"),
                gte("score", 0.8),
                ne("status", "deleted")
            )

            val result = converter.convert(filter)

            assertEquals(
                "(e.owner = \$_filter_0) AND (e.score >= \$_filter_1) AND (e.status <> \$_filter_2)",
                result.whereClause
            )
            assertEquals(
                mapOf(
                    "_filter_0" to "alice",
                    "_filter_1" to 0.8,
                    "_filter_2" to "deleted"
                ),
                result.parameters
            )
        }
    }

    @Nested
    inner class CustomNodeAliasTests {

        @Test
        fun `custom node alias is used`() {
            val customConverter = CypherFilterConverter(nodeAlias = "chunk")

            val result = customConverter.convert(eq("text", "hello"))

            assertEquals("chunk.text = \$_filter_0", result.whereClause)
        }

        @Test
        fun `custom param prefix is used`() {
            val customConverter = CypherFilterConverter(nodeAlias = "n", paramPrefix = "_p_")

            val result = customConverter.convert(eq("owner", "alice"))

            assertEquals("n.owner = \$_p_0", result.whereClause)
            assertEquals(mapOf("_p_0" to "alice"), result.parameters)
        }
    }

    @Nested
    inner class AppendToTests {

        @Test
        fun `appendTo with empty existing clause`() {
            val result = converter.convert(eq("owner", "alice"))

            val appended = result.appendTo("")

            assertEquals("e.owner = \$_filter_0", appended)
        }

        @Test
        fun `appendTo with existing clause`() {
            val result = converter.convert(eq("owner", "alice"))

            val appended = result.appendTo("score >= 0.5")

            assertEquals("score >= 0.5 AND e.owner = \$_filter_0", appended)
        }

        @Test
        fun `appendTo with empty filter returns existing clause`() {
            val result = converter.convert(null)

            val appended = result.appendTo("score >= 0.5")

            assertEquals("score >= 0.5", appended)
        }
    }

    @Nested
    inner class ValueTypeTests {

        @Test
        fun `string values are preserved`() {
            val result = converter.convert(eq("name", "Test Name"))

            assertEquals("Test Name", result.parameters["_filter_0"])
        }

        @Test
        fun `integer values are preserved`() {
            val result = converter.convert(eq("count", 42))

            assertEquals(42, result.parameters["_filter_0"])
        }

        @Test
        fun `double values are preserved`() {
            val result = converter.convert(eq("score", 0.95))

            assertEquals(0.95, result.parameters["_filter_0"])
        }

        @Test
        fun `boolean values are preserved`() {
            val result = converter.convert(eq("active", true))

            assertEquals(true, result.parameters["_filter_0"])
        }

        @Test
        fun `list values are preserved for In`() {
            val result = converter.convert(PropertyFilter.`in`("tags", listOf("a", "b", "c")))

            assertEquals(listOf("a", "b", "c"), result.parameters["_filter_0"])
        }
    }

    @Nested
    inner class ParameterCountingTests {

        @Test
        fun `parameters are numbered sequentially`() {
            val filter = PropertyFilter.and(
                eq("a", "1"),
                eq("b", "2"),
                eq("c", "3")
            )

            val result = converter.convert(filter)

            assertTrue(result.parameters.containsKey("_filter_0"))
            assertTrue(result.parameters.containsKey("_filter_1"))
            assertTrue(result.parameters.containsKey("_filter_2"))
            assertFalse(result.parameters.containsKey("_filter_3"))
        }
    }
}
