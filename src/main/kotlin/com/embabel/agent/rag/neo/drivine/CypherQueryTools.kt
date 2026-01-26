package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.api.annotation.LlmTool
import com.embabel.agent.api.tool.AgenticTool
import com.embabel.agent.api.tool.Tool
import com.embabel.agent.core.DataDictionary
import com.embabel.agent.rag.model.NamedEntityData
import com.embabel.agent.rag.neo.drivine.mappers.NamedEntityDataRowMapper
import com.embabel.common.ai.model.LlmOptions
import com.embabel.common.util.loggerFor
import org.drivine.manager.PersistenceManager
import org.drivine.mapper.RowMapper
import org.drivine.query.QuerySpecification

/**
 * A compiled Cypher query ready for validation and execution.
 */
data class CompiledCypherQuery(
    val query: String,
)

/**
 * Strategy interface for validating Cypher queries before execution.
 */
interface CypherQueryValidator {

    /**
     * Validates a Cypher query.
     * @param query The compiled query to validate
     * @param schema The data dictionary containing schema information
     * @throws IllegalArgumentException if the query is invalid
     */
    fun validate(query: CompiledCypherQuery, schema: DataDictionary)
}

/**
 * Safe tools for building Cypher queries based on the provided schema.
 *
 * Provides an agentic tool that uses LLM to generate and execute Cypher queries
 * against a Neo4j graph database. The tool supports three query types:
 * - Count queries: Returns counts based on the user's question
 * - Entity queries: Returns entities matching the user's question
 * - Value queries: Returns literal values as strings based on the user's question
 *
 * All queries are validated against the provided schema before execution.
 *
 * Usage:
 * ```kotlin
 * val cypherTools = CypherQueryTools(schema, persistenceManager)
 * val tool = cypherTools.tool("Query the knowledge graph")
 * // Use tool with your agent
 * ```
 */
class CypherQueryTools @JvmOverloads constructor(
    val schema: DataDictionary,
    private val persistenceManager: PersistenceManager,
    val llm: LlmOptions = LlmOptions(),
    private val namedEntityDataMapper: RowMapper<NamedEntityData> = NamedEntityDataRowMapper(),
    private val validator: CypherQueryValidator = NoMutationValidator(),
) {

    /**
     * Returns the main agentic tool that uses LLM to generate and execute Cypher queries.
     * This is the only public entry point - the internal LLM tools are not directly accessible.
     *
     * @param description Description of what this tool does in the context of your application
     * @param name Optional custom name for the tool (default: "cypher_query_tool")
     */
    @JvmOverloads
    fun tool(description: String, name: String = "cypher_query_tool"): Tool {
        val executor = CypherToolExecutor(
            schema = schema,
            persistenceManager = persistenceManager,
            namedEntityDataMapper = namedEntityDataMapper,
            validator = validator,
        )
        return AgenticTool(
            name = name,
            description = description,
        )
            .withParameter(
                Tool.Parameter.string("question", "The user's question to answer using the graph database")
            )
            .withSystemPrompt(buildSystemPrompt())
            .withTools(*Tool.fromInstance(executor).toTypedArray())
            .withLlm(llm)
    }

    private fun buildSystemPrompt(): String {
        val entityTypesSection = schema.domainTypes.joinToString("\n") { domainType ->
            "- ${domainType.ownLabel}: ${domainType.description}"
        }

        val relationshipsSection = schema.allowedRelationships().joinToString("\n") { rel ->
            "- (:${rel.from.ownLabel})-[:${rel.name}]->(:${rel.to.ownLabel}): ${rel.description}"
        }

        val propertiesSection = schema.domainTypes.joinToString("\n") { domainType ->
            val props = domainType.properties.joinToString(", ") { it.name }
            "- ${domainType.ownLabel}: $props"
        }

        return """
            You are a Cypher query generator for Neo4j. Generate safe, read-only Cypher queries
            based on the user's question and the provided schema.

            ## Graph Schema

            ### Entity Types (Node Labels)
            $entityTypesSection

            ### Relationship Types
            $relationshipsSection

            ### Properties by Entity Type
            $propertiesSection

            ## Rules
            - Only generate READ queries (MATCH, RETURN, WITH, WHERE, ORDER BY, LIMIT)
            - Never generate WRITE queries (CREATE, MERGE, SET, DELETE, REMOVE)
            - Use the entity types and relationships defined in the schema above
            - For count queries: use cypher_count tool, query must return COUNT() aliased as 'count'
            - For entity queries: use query_for_entities tool, return entity properties as a map
            - For value queries: use query_for_values tool, return values aliased as 'value'
            - For aggregations, GROUP BY, or tabular data: use query_for_rows tool

            ## Query Patterns

            ### Count Examples
            ```cypher
            MATCH (n:Person) RETURN count(n) AS count
            MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS count
            ```

            ### Entity Examples
            ```cypher
            MATCH (e:Entity)
            WHERE e.name CONTAINS 'search term'
            RETURN {
                id: e.id,
                name: e.name,
                description: e.description,
                labels: labels(e),
                properties: properties(e)
            } AS result
            ```

            ### Value Examples
            ```cypher
            MATCH (p:Person) RETURN p.name AS value
            MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name AS value
            ```

            ### Rows/Aggregation Examples
            For aggregations and GROUP BY queries, always return results as a single map column:
            ```cypher
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            WITH c.name AS company, count(p) AS employeeCount
            RETURN {company: company, employeeCount: employeeCount} AS row
            ORDER BY employeeCount DESC

            MATCH (p:Person)
            WITH p.department AS department, avg(p.salary) AS avgSalary, count(p) AS cnt
            RETURN {department: department, avgSalary: avgSalary, count: cnt} AS row
            ```
        """.trimIndent()
    }
}

/**
 * Internal executor class containing the @LlmTool annotated methods.
 * This class is not exposed to users - they only interact with [CypherQueryTools.tool].
 */
internal class CypherToolExecutor(
    private val schema: DataDictionary,
    private val persistenceManager: PersistenceManager,
    private val namedEntityDataMapper: RowMapper<NamedEntityData>,
    private val validator: CypherQueryValidator,
) {

    private val logger = loggerFor<CypherToolExecutor>()

    @LlmTool(
        name = "cypher_count",
        description = """
            Execute a Cypher query that returns a count.
            Use this when the user wants to know 'how many' of something exists.
            The query must return a single integer count aliased as 'count'.
        """
    )
    fun cypherCount(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. Must return a single count value aliased as 'count'.
                Example: MATCH (n:Person) RETURN count(n) AS count
            """
        )
        cypher: String,
    ): String {
        return try {
            val compiled = CompiledCypherQuery(cypher)
            validator.validate(compiled, schema)

            logger.info("Executing count query: {}", cypher)
            val result = executeForCount(compiled)
            logger.info("Count result: {}", result)
            "Count: $result"
        } catch (e: Exception) {
            val errorMessage = "Error executing count query: ${e.message}"
            logger.error(errorMessage, e)
            """
                ERROR: $errorMessage
                Please fix the query and try again.
            """.trimIndent()
        }
    }

    @LlmTool(
        name = "query_for_entities",
        description = """
            Execute a Cypher query that returns entities from the graph.
            Use this when the user wants to find or list entities (nodes) in the database.
            The query must return entity data with id, name, description, labels, and properties.
        """
    )
    fun queryForEntities(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. Must return a map with id, name, description, labels, properties aliased as 'result'.
                Example: MATCH (e:Person) RETURN {id: e.id, name: e.name, description: e.description, labels: labels(e), properties: properties(e)} AS result
            """
        )
        cypher: String,
    ): String {
        return try {
            val compiled = CompiledCypherQuery(cypher)
            validator.validate(compiled, schema)

            logger.info("Executing entity query: {}", cypher)
            val results = executeForEntities(compiled)
            logger.info("Found {} entities", results.size)

            if (results.isEmpty()) {
                "No entities found."
            } else {
                "Found ${results.size} entities:\n" + results.joinToString("\n") { entity ->
                    "- ${entity.name} (${entity.labels().joinToString(", ")}): ${entity.description}"
                }
            }
        } catch (e: Exception) {
            val errorMessage = "Error executing entity query: ${e.message}"
            logger.error(errorMessage, e)
            """
                ERROR: $errorMessage
                Please fix the query and try again.
            """.trimIndent()
        }
    }

    @LlmTool(
        name = "query_for_values",
        description = """
            Execute a Cypher query that returns literal values (strings, numbers, etc.).
            Use this when the user wants specific property values, not full entities.
            The query must return values aliased as 'value'.
        """
    )
    fun queryForValues(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. Must return values aliased as 'value'.
                Example: MATCH (p:Person) RETURN p.name AS value
            """
        )
        cypher: String,
    ): String {
        return try {
            val compiled = CompiledCypherQuery(cypher)
            validator.validate(compiled, schema)

            logger.info("Executing values query: {}", cypher)
            val results = executeForValues(compiled)
            logger.info("Found {} values", results.size)

            if (results.isEmpty()) {
                "No values found."
            } else {
                "Found ${results.size} values:\n" + results.joinToString("\n") { "- $it" }
            }
        } catch (e: Exception) {
            val errorMessage = "Error executing values query: ${e.message}"
            logger.error(errorMessage, e)
            """
                ERROR: $errorMessage
                Please fix the query and try again.
            """.trimIndent()
        }
    }

    @LlmTool(
        name = "query_for_rows",
        description = """
            Execute a Cypher query that returns arbitrary rows with multiple columns.
            Use this for aggregations, GROUP BY queries, or any query returning tabular data.
            Returns each row as a map of column names to values.
            IMPORTANT: The query must return a single map column (not multiple columns).
        """
    )
    fun queryForRows(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. Must return results as a single map column named 'row'.
                Example: MATCH (p:Person)-[:WORKS_AT]->(c:Company) WITH c.name AS company, count(p) AS cnt RETURN {company: company, count: cnt} AS row ORDER BY cnt DESC
            """
        )
        cypher: String,
    ): String {
        return try {
            val compiled = CompiledCypherQuery(cypher)
            validator.validate(compiled, schema)

            logger.info("Executing rows query: {}", cypher)
            val results = executeForRows(compiled)
            logger.info("Found {} rows", results.size)

            if (results.isEmpty()) {
                "No results found."
            } else {
                "Found ${results.size} rows:\n" + results.joinToString("\n") { row ->
                    row.entries.joinToString(", ") { (k, v) -> "$k: $v" }
                }
            }
        } catch (e: Exception) {
            val errorMessage = "Error executing rows query: ${e.message}"
            logger.error(errorMessage, e)
            """
                ERROR: $errorMessage
                Please fix the query and try again.
            """.trimIndent()
        }
    }

    private fun executeForCount(query: CompiledCypherQuery): Int {
        return persistenceManager.getOne(
            QuerySpecification
                .withStatement(query.query)
                .transform(Int::class.java)
        )
    }

    private fun executeForEntities(query: CompiledCypherQuery): List<NamedEntityData> {
        return persistenceManager.query(
            QuerySpecification
                .withStatement(query.query)
                .mapWith(namedEntityDataMapper)
        )
    }

    private fun executeForValues(query: CompiledCypherQuery): List<String> {
        @Suppress("UNCHECKED_CAST")
        val results = persistenceManager.query(
            QuerySpecification
                .withStatement(query.query)
                .transform(Map::class.java)
        ) as List<Map<String, Any>>

        return results.mapNotNull { row ->
            row["value"]?.toString()
        }
    }

    private fun executeForRows(query: CompiledCypherQuery): List<Map<String, Any>> {
        @Suppress("UNCHECKED_CAST")
        return persistenceManager.query(
            QuerySpecification
                .withStatement(query.query)
                .transform(Map::class.java)
        ) as List<Map<String, Any>>
    }
}
