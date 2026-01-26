package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.core.DataDictionary
import com.embabel.common.util.loggerFor
import org.slf4j.Logger

/**
 * Default validator that ensures queries are read-only (no mutations).
 *
 * Validates:
 * - No write operations (CREATE, MERGE, SET, DELETE, REMOVE, DETACH, DROP)
 * - No write procedures (APOC.CREATE, APOC.MERGE)
 * - Query starts with valid read operations (MATCH, OPTIONAL, WITH, CALL, RETURN, UNWIND)
 * - Warns on entity types and relationships not found in schema
 */
class NoMutationValidator : CypherQueryValidator {

    private val logger = loggerFor<NoMutationValidator>()

    override fun validate(query: CompiledCypherQuery, schema: DataDictionary) {
        val cypher = query.query.uppercase()

        validateNoWriteOperations(cypher)
        validateNoWriteProcedures(cypher)
        validateStartsWithReadOperation(cypher)
        validateEntityTypes(query.query, schema, logger)
        validateRelationshipTypes(query.query, schema, logger)

        logger.debug("Query validated successfully: {}", query.query)
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

    companion object {

        /**
         * Validates that entity types used in the query exist in the schema.
         * Logs a warning if unknown types are found but does not throw.
         */
        fun validateEntityTypes(cypher: String, schema: DataDictionary, logger: Logger) {
            // Extract node labels from patterns like (n:Label) or (n:Label1:Label2)
            val labelPattern = "\\(\\w*:([A-Za-z_][A-Za-z0-9_]*)".toRegex()
            val usedLabels = labelPattern.findAll(cypher).map { it.groupValues[1] }.toSet()

            val schemaLabels = schema.domainTypes.flatMap { it.labels }.toSet()
            val unknownLabels = usedLabels - schemaLabels

            if (unknownLabels.isNotEmpty()) {
                logger.warn(
                    "Query uses entity types not in schema: {}. Available types: {}",
                    unknownLabels,
                    schemaLabels
                )
            }
        }

        /**
         * Validates that relationship types used in the query exist in the schema.
         * Logs a warning if unknown types are found but does not throw.
         */
        fun validateRelationshipTypes(cypher: String, schema: DataDictionary, logger: Logger) {
            // Extract relationship types from patterns like -[:REL_TYPE]-> or -[:REL_TYPE*]->
            val relPattern = "\\[:([A-Za-z_][A-Za-z0-9_]*)".toRegex()
            val usedRelTypes = relPattern.findAll(cypher).map { it.groupValues[1] }.toSet()

            val schemaRelTypes = schema.allowedRelationships().map { it.name }.toSet()
            val unknownRelTypes = usedRelTypes - schemaRelTypes

            if (unknownRelTypes.isNotEmpty()) {
                logger.warn(
                    "Query uses relationship types not in schema: {}. Available types: {}",
                    unknownRelTypes,
                    schemaRelTypes
                )
            }
        }
    }
}