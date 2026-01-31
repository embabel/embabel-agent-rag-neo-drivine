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
package com.embabel.agent.rag.neo.drivine.cyphergen

import com.embabel.agent.core.DataDictionary
import com.embabel.common.util.loggerFor

/**
 * Validator that ensures queries are read-only (no mutations).
 *
 * Validates:
 * - No write operations (CREATE, MERGE, SET, DELETE, REMOVE, DETACH, DROP)
 * - No write procedures (APOC.CREATE, APOC.MERGE)
 * - Query starts with valid read operations (MATCH, OPTIONAL, WITH, CALL, RETURN, UNWIND)
 */
object NoMutationValidator : CypherQueryValidator {

    private val logger = loggerFor<NoMutationValidator>()

    override fun validate(query: CompiledCypherQuery, schema: DataDictionary) {
        val cypher = query.query.uppercase()

        validateNoWriteOperations(cypher)
        validateNoWriteProcedures(cypher)
        validateStartsWithReadOperation(cypher)

        logger.debug("Query validated successfully (no mutations): {}", query.query)
    }

    private fun validateNoWriteOperations(cypher: String) {
        val writeKeywords = listOf("CREATE", "MERGE", "SET", "DELETE", "REMOVE", "DETACH", "DROP")
        for (keyword in writeKeywords) {
            // Use word boundary check to avoid false positives (e.g., "RESET" containing "SET")
            val pattern = "\\b$keyword\\b".toRegex()
            if (pattern.containsMatchIn(cypher)) {
                throw IllegalArgumentException("Write operations are not allowed. Found: $keyword")
            }
        }
    }

    private fun validateNoWriteProcedures(cypher: String) {
        if (cypher.contains("CALL") && (cypher.contains("APOC.CREATE") || cypher.contains("APOC.MERGE"))) {
            throw IllegalArgumentException("Write procedures are not allowed")
        }
    }

    private fun validateStartsWithReadOperation(cypher: String) {
        val trimmedCypher = cypher.trim()
        val validStartKeywords = listOf("MATCH", "OPTIONAL", "WITH", "CALL", "RETURN", "UNWIND")
        val startsWithValid = validStartKeywords.any { trimmedCypher.startsWith(it) }
        if (!startsWithValid) {
            throw IllegalArgumentException(
                "Query must start with a valid read operation (MATCH, OPTIONAL MATCH, WITH, CALL, RETURN, UNWIND)"
            )
        }
    }
}
