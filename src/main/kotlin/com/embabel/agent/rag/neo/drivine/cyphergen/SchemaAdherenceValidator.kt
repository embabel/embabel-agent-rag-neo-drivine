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
 * Validator that checks queries use only entity types and relationships defined in the schema.
 *
 * @param strict If true (default), throws an exception on unknown labels/relationships.
 *               If false, only logs warnings.
 */
class SchemaAdherenceValidator(
    private val strict: Boolean = true,
) : CypherQueryValidator {

    private val logger = loggerFor<SchemaAdherenceValidator>()

    override fun validate(query: CompiledCypherQuery, schema: DataDictionary) {
        validateEntityTypes(query.query, schema)
        validateRelationshipTypes(query.query, schema)
    }

    private fun validateEntityTypes(cypher: String, schema: DataDictionary) {
        // Extract node labels from patterns like (n:Label) or (n:Label1:Label2)
        val labelPattern = "\\(\\w*:([A-Za-z_][A-Za-z0-9_]*)".toRegex()
        val usedLabels = labelPattern.findAll(cypher).map { it.groupValues[1] }.toSet()

        val schemaLabels = schema.domainTypes.flatMap { it.labels }.toSet()
        val unknownLabels = usedLabels - schemaLabels

        if (unknownLabels.isNotEmpty()) {
            val message = "Query uses entity types not in schema: $unknownLabels. Available types: $schemaLabels"
            if (strict) {
                throw IllegalArgumentException(message)
            } else {
                logger.warn(message)
            }
        }
    }

    private fun validateRelationshipTypes(cypher: String, schema: DataDictionary) {
        // Extract relationship types from patterns like -[:REL_TYPE]-> or -[:REL_TYPE*]->
        val relPattern = "\\[:([A-Za-z_][A-Za-z0-9_]*)".toRegex()
        val usedRelTypes = relPattern.findAll(cypher).map { it.groupValues[1] }.toSet()

        val schemaRelTypes = schema.allowedRelationships().map { it.name }.toSet()
        val unknownRelTypes = usedRelTypes - schemaRelTypes

        if (unknownRelTypes.isNotEmpty()) {
            val message =
                "Query uses relationship types not in schema: $unknownRelTypes. Available types: $schemaRelTypes"
            if (strict) {
                throw IllegalArgumentException(message)
            } else {
                logger.warn(message)
            }
        }
    }
}
