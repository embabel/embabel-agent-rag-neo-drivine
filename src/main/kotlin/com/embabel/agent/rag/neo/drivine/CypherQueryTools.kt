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
 * Normalizes the query by converting literal escape sequences to actual characters,
 * handling cases where LLMs double-escape newlines in JSON output.
 */
data class CompiledCypherQuery(
    val query: String,
) {
    companion object {
        /**
         * Creates a CompiledCypherQuery with normalized whitespace.
         * Converts literal \n and \t sequences to actual newlines/tabs.
         */
        operator fun invoke(rawQuery: String): CompiledCypherQuery {
            val normalized = rawQuery
                .replace("\\n", "\n")
                .replace("\\t", "\t")
            return CompiledCypherQuery(normalized)
        }
    }
}

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
    private val validator: CypherQueryValidator = ChainedQueryValidator(
        NoMutationValidator,
        SchemaAdherenceValidator(strict = true),
    ),
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

            ## CRITICAL: Entity Resolution Rule
            **NEVER guess or fabricate entity names or IDs from general knowledge.**
            **ALWAYS call find_entity FIRST** to search the database for any entity mentioned by the user.

            Example: User asks "how many works did biber write for violin"
            - WRONG: Immediately query with guessed name "Heinrich Ignaz Franz Biber" or ID "biber-1644"
            - RIGHT: First call find_entity(label="Composer", searchTerm="biber") to get the actual ID/name

            ## Rules
            - Only generate READ queries (MATCH, RETURN, WITH, WHERE, ORDER BY, LIMIT)
            - Never generate WRITE queries (CREATE, MERGE, SET, DELETE, REMOVE)
            - Use the entity types and relationships defined in the schema above

            ## Tool Selection Guide

            ### Discovery Tools (use these FIRST to explore the data)
            - **list_all**: See all entities of a type
              - Use for: "What ensembles exist?", "What instruments are available?"
              - Call: `list_all(label="Ensemble")` or `list_all(label="Instrument")`

            - **describe_label**: Understand relationships for a label
              - Use for: "How do Works connect to other entities?"
              - Call: `describe_label(label="Work")`

            - **get_by_id**: Get full details of a specific entity
              - Use for: Looking up properties of a known entity
              - Call: `get_by_id(label="Ensemble", id="string-quartet")`

            - **find_entity**: Search for an entity by partial name
              - Use for: Resolving user-mentioned names to exact IDs
              - Call: `find_entity(label="Composer", searchTerm="mozart")`

            ### Query Tools (use after you know the IDs/structure)
            - **cypher_count**: ONLY for simple counts returning a single number
              - Use for: "How many works did Mozart compose?"
              - Query MUST be: `RETURN count(...) AS count` (single value only!)

            - **query_for_rows**: For rankings, aggregations, "most/least" questions
              - Use for: "Who composed the most symphonies?" or "Top 10 composers by work count"
              - Query returns: `RETURN {name: x, count: y} AS row ORDER BY ...`

            - **query_for_entities**: For listing/finding a SINGLE entity type with full details
              - Use for: "Find works by Beethoven", "List all symphonies"
              - ONLY returns one entity type per query (just Works OR just Composers, not both)
              - MUST use exact key names: id, name, description, labels, properties

            - **query_for_rows**: For queries returning multiple entity types or custom data
              - Use for: "Find works with their composers", "Show work title and composer name together"
              - Use when you need custom column names or multiple entities in one row

            - **query_for_values**: For simple property lookups
              - Use for: "What is Mozart's birth year?"

            ## Recommended Workflow
            1. Use **list_all** or **describe_label** to discover what's in the database
            2. Use **find_entity** to resolve any names the user mentions
            3. Use the appropriate query tool with the correct IDs

            ## Required Query Flow
            1. **FIRST**: Call find_entity to resolve any entity names mentioned by the user
            2. **THEN**: Use the exact name or id returned by find_entity in your query
            3. **NEVER** skip step 1 - even if you think you know the full name

            ## Text Search Best Practices
            - ALWAYS use toLower() for case-insensitive matching: `WHERE toLower(w.title) CONTAINS 'violin'`
            - Search multiple text fields when looking for keywords: title, subtitle, searchTerms, name
            - Example: `WHERE toLower(w.title) CONTAINS 'violin' OR toLower(w.searchTerms) CONTAINS 'violin'`

            ## Query Patterns

            ### Count Examples
            ```cypher
            MATCH (n:Person) RETURN count(n) AS count
            MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS count

            // Count works by a composer containing a keyword (case-insensitive, multiple fields)
            MATCH (c:Composer {id: 'composer-id'})-[:COMPOSED]->(w:Work)
            WHERE toLower(w.title) CONTAINS 'violin'
               OR toLower(w.searchTerms) CONTAINS 'violin'
            RETURN count(w) AS count
            ```

            ### Entity Examples (query_for_entities)
            CRITICAL: The returned map MUST use these EXACT key names: id, name, description, labels, properties
            Do NOT use variations like workId, workName, composerId, etc. - these will cause errors.
            Only return ONE entity type per query. For multiple entity types, use query_for_rows instead.

            CORRECT - single entity with exact key names:
            ```cypher
            MATCH (w:Work)
            WHERE w.title CONTAINS 'symphony'
            RETURN {
                id: w.id,
                name: w.name,
                description: w.subtitle,
                labels: labels(w),
                properties: properties(w)
            } AS result
            ```

            WRONG - do NOT do this (wrong key names):
            ```cypher
            RETURN {workId: w.id, workName: w.name} AS result  // WRONG: use 'id' and 'name'
            ```

            WRONG - do NOT do this (multiple entity types):
            ```cypher
            RETURN {id: w.id, name: w.name, composerId: c.id} AS result  // WRONG: use query_for_rows
            ```

            ### Value Examples
            ```cypher
            MATCH (p:Person) RETURN p.name AS value
            MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name AS value
            ```

            ### Rows/Aggregation Examples
            For aggregations and GROUP BY queries, always return results as a single map column:
            ```cypher
            // Top composers by work count
            MATCH (c:Composer)-[:COMPOSED]->(w:Work)
            WITH c, count(w) AS workCount
            RETURN {composer: c.name, workCount: workCount} AS row
            ORDER BY workCount DESC
            LIMIT 10

            // Works scored for a specific ensemble
            MATCH (w:Work)-[:SCORED_FOR]->(e:Ensemble {id: 'string-quartet'})
            MATCH (c:Composer)-[:COMPOSED]->(w)
            WITH c, count(w) AS quartetCount
            RETURN {composer: c.name, count: quartetCount} AS row
            ORDER BY quartetCount DESC

            // Works scored for a specific instrument
            MATCH (w:Work)-[:SCORED_FOR]->(i:Instrument {id: 'violin'})
            MATCH (c:Composer)-[:COMPOSED]->(w)
            WITH c, count(DISTINCT w) AS count
            RETURN {composer: c.name, count: count} AS row
            ORDER BY count DESC
            ```

            ### Finding Ensembles and Instruments
            ```cypher
            // Find all ensembles
            MATCH (e:Ensemble) RETURN e.id, e.name

            // Find instruments in an ensemble
            MATCH (e:Ensemble {id: 'string-quartet'})-[:CONTAINS]->(i:Instrument)
            RETURN i.name

            // Find instrument families
            MATCH (i:Instrument)-[:OF_FAMILY]->(f:Family)
            RETURN i.name, f.name
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
        name = "find_entity",
        description = """
            Search for an entity by name using case-insensitive partial matching.
            Use this FIRST when the user mentions an entity (composer, person, work, etc.) by name.
            This resolves informal or partial names to the exact entity in the database.
            Returns matching entities with their IDs and full names.

            If multiple results are returned, choose the most relevant one for your query.
            If no results or unsatisfactory results, try again with a shorter/different search term.
        """
    )
    fun findEntity(
        @LlmTool.Param(
            description = "The node label to search (e.g., 'Composer', 'Work', 'Person')"
        )
        label: String,
        @LlmTool.Param(
            description = "The search term - can be partial name, case-insensitive (e.g., 'biber', 'mozart', 'beethoven')"
        )
        searchTerm: String,
    ): String {
        return try {
            val cypher = """
                MATCH (n:$label)
                WHERE toLower(n.name) CONTAINS toLower('$searchTerm')
                RETURN {
                    id: n.id,
                    name: n.name,
                    labels: labels(n)
                } AS result
                LIMIT 10
            """.trimIndent()

            logger.info("Finding entity with label={}, searchTerm={}", label, searchTerm)
            val compiled = CompiledCypherQuery(cypher)

            @Suppress("UNCHECKED_CAST")
            val results = persistenceManager.query(
                QuerySpecification
                    .withStatement(compiled.query)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            logger.info("Found {} matching entities", results.size)

            if (results.isEmpty()) {
                """
                    No $label found matching '$searchTerm'.
                    Try again with a different or shorter search term (e.g., just the surname).
                """.trimIndent()
            } else if (results.size == 1) {
                val result = results[0]
                val id = result["id"] ?: "unknown"
                val name = result["name"] ?: "unknown"
                "Found exact match: $name (id: $id). Use this id or name in your query."
            } else {
                "Found ${results.size} matching $label(s). Choose the most relevant one:\n" +
                        results.joinToString("\n") { result ->
                            val id = result["id"] ?: "unknown"
                            val name = result["name"] ?: "unknown"
                            "- $name (id: $id)"
                        } + "\n\nUse the chosen entity's id or exact name in your query."
            }
        } catch (e: Exception) {
            val errorMessage = "Error finding entity: ${e.message}"
            logger.error(errorMessage, e)
            """
                ERROR: $errorMessage
                Please check the label name and try again with a different search term.
            """.trimIndent()
        }
    }

    @LlmTool(
        name = "list_all",
        description = """
            List all entities of a given type/label.
            Use this to discover what entities exist in the database before writing queries.
            Returns IDs and names of all entities with the specified label.

            Example uses:
            - See all available Ensemble types (string quartet, orchestra, etc.)
            - See all Instrument types
            - See all Technique types
        """
    )
    fun listAll(
        @LlmTool.Param(
            description = "The node label to list (e.g., 'Ensemble', 'Instrument', 'Family', 'Technique', 'Composer')"
        )
        label: String,
        @LlmTool.Param(
            description = "Maximum number of results to return (default 50)"
        )
        limit: Int = 50,
    ): String {
        return try {
            val cypher = """
                MATCH (n:$label)
                RETURN n.id AS id, n.name AS name
                ORDER BY n.name
                LIMIT $limit
            """.trimIndent()

            logger.info("Listing all {} (limit {})", label, limit)

            @Suppress("UNCHECKED_CAST")
            val results = persistenceManager.query(
                QuerySpecification
                    .withStatement(cypher)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            logger.info("Found {} {} entities", results.size, label)

            if (results.isEmpty()) {
                "No $label entities found in the database."
            } else {
                "Found ${results.size} $label entities:\n" +
                    results.joinToString("\n") { row ->
                        val id = row["id"] ?: "?"
                        val name = row["name"] ?: "?"
                        "- $name (id: $id)"
                    }
            }
        } catch (e: Exception) {
            val errorMessage = "Error listing $label: ${e.message}"
            logger.error(errorMessage, e)
            "ERROR: $errorMessage"
        }
    }

    @LlmTool(
        name = "get_by_id",
        description = """
            Get a single entity by its exact ID.
            Use this when you already know the entity's ID and want to see its properties.
            Returns all properties of the entity.
        """
    )
    fun getById(
        @LlmTool.Param(
            description = "The node label (e.g., 'Composer', 'Work', 'Ensemble')"
        )
        label: String,
        @LlmTool.Param(
            description = "The exact ID of the entity"
        )
        id: String,
    ): String {
        return try {
            val cypher = """
                MATCH (n:$label {id: '$id'})
                RETURN properties(n) AS props, labels(n) AS labels
            """.trimIndent()

            logger.info("Getting {} by id={}", label, id)

            @Suppress("UNCHECKED_CAST")
            val results = persistenceManager.query(
                QuerySpecification
                    .withStatement(cypher)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            if (results.isEmpty()) {
                "No $label found with id '$id'."
            } else {
                val row = results[0]
                val props = row["props"] as? Map<*, *> ?: emptyMap<String, Any>()
                val labels = row["labels"] as? List<*> ?: emptyList<String>()

                "Found $label (labels: ${labels.joinToString(", ")}):\n" +
                    props.entries.joinToString("\n") { (k, v) -> "  $k: $v" }
            }
        } catch (e: Exception) {
            val errorMessage = "Error getting $label by id: ${e.message}"
            logger.error(errorMessage, e)
            "ERROR: $errorMessage"
        }
    }

    @LlmTool(
        name = "describe_label",
        description = """
            Describe a node label's relationships in the database.
            Use this to understand how a label connects to other entities.
            Shows outgoing and incoming relationship types with their target labels.
        """
    )
    fun describeLabel(
        @LlmTool.Param(
            description = "The node label to describe (e.g., 'Work', 'Composer', 'Ensemble')"
        )
        label: String,
    ): String {
        return try {
            // Find outgoing relationships
            val outgoingCypher = """
                MATCH (n:$label)-[r]->(m)
                WITH type(r) AS relType, labels(m) AS targetLabels
                RETURN DISTINCT relType, targetLabels
                LIMIT 20
            """.trimIndent()

            // Find incoming relationships
            val incomingCypher = """
                MATCH (n:$label)<-[r]-(m)
                WITH type(r) AS relType, labels(m) AS sourceLabels
                RETURN DISTINCT relType, sourceLabels
                LIMIT 20
            """.trimIndent()

            logger.info("Describing label {}", label)

            @Suppress("UNCHECKED_CAST")
            val outgoing = persistenceManager.query(
                QuerySpecification
                    .withStatement(outgoingCypher)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            @Suppress("UNCHECKED_CAST")
            val incoming = persistenceManager.query(
                QuerySpecification
                    .withStatement(incomingCypher)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            val result = StringBuilder("Label: $label\n")

            if (outgoing.isNotEmpty()) {
                result.append("\nOutgoing relationships:\n")
                outgoing.forEach { row ->
                    val relType = row["relType"]
                    val targets = (row["targetLabels"] as? List<*>)?.joinToString(", ") ?: "?"
                    result.append("  -[:$relType]-> ($targets)\n")
                }
            }

            if (incoming.isNotEmpty()) {
                result.append("\nIncoming relationships:\n")
                incoming.forEach { row ->
                    val relType = row["relType"]
                    val sources = (row["sourceLabels"] as? List<*>)?.joinToString(", ") ?: "?"
                    result.append("  <-[:$relType]- ($sources)\n")
                }
            }

            if (outgoing.isEmpty() && incoming.isEmpty()) {
                result.append("No relationships found for this label.")
            }

            result.toString()
        } catch (e: Exception) {
            val errorMessage = "Error describing $label: ${e.message}"
            logger.error(errorMessage, e)
            "ERROR: $errorMessage"
        }
    }

    @LlmTool(
        name = "cypher_count",
        description = """
            Execute a Cypher query that returns a COUNT.
            Use this ONLY when you need a simple count of matching nodes or relationships.

            IMPORTANT: The query MUST end with: RETURN count(...) AS count
            Do NOT use this for queries that return multiple columns or rows with data.
            For "who has the most" or "top N" questions, use query_for_rows instead.
        """
    )
    fun cypherCount(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. MUST return a single count value aliased as 'count'.

                CORRECT examples:
                - MATCH (n:Person) RETURN count(n) AS count
                - MATCH (c:Composer)-[:COMPOSED]->(w:Work) WHERE c.id = 'mozart' RETURN count(w) AS count

                WRONG (do NOT do this - use query_for_rows instead):
                - MATCH ... RETURN c.name, count(w) AS count  -- multiple columns!
                - MATCH ... WITH c, count(w) AS cnt RETURN c.name, cnt  -- multiple columns!
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
            val errorMessage = e.message ?: "Unknown error"
            logger.error("Error executing count query: {}", errorMessage, e)

            // Provide specific guidance based on the error
            val guidance = when {
                errorMessage.contains("Expected exactly one result") ->
                    "Your query returns multiple rows or columns. Use query_for_rows for aggregations that return multiple results."
                errorMessage.contains("Invalid input") ->
                    "Cypher syntax error. Check your query syntax and try again."
                else -> "Please fix the query and try again."
            }

            """
                ERROR: $errorMessage
                $guidance
            """.trimIndent()
        }
    }

    @LlmTool(
        name = "query_for_entities",
        description = """
            Execute a Cypher query that returns entities from the graph.
            Use this ONLY when returning a SINGLE entity type (e.g., just Works or just Composers).
            For queries involving multiple entity types or custom columns, use query_for_rows instead.

            CRITICAL: The returned map MUST use these EXACT key names: id, name, description, labels, properties.
            Do NOT use variations like workId, workName, composerId - this will cause errors.
        """
    )
    fun queryForEntities(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. MUST return a map with these EXACT keys aliased as 'result':
                - id: the entity's id (MUST be named 'id', not 'workId' or 'composerId')
                - name: the entity's name (MUST be named 'name', not 'workName' or 'title')
                - description: optional description
                - labels: labels(node)
                - properties: properties(node)

                CORRECT: RETURN {id: w.id, name: w.name, description: w.subtitle, labels: labels(w), properties: properties(w)} AS result
                WRONG: RETURN {workId: w.id, workName: w.name} AS result
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
            Execute a Cypher query that returns rows of data.
            Use this for:
            - Rankings ("who has the most...", "top 10...")
            - Aggregations with grouping
            - Any query returning multiple columns or multiple results

            Returns each row as a map of column names to values.
        """
    )
    fun queryForRows(
        @LlmTool.Param(
            description = """
                The Cypher query to execute. Should return results as a map column named 'row'.
                Example: MATCH (c:Composer)-[:COMPOSED]->(w:Work) WITH c, count(w) AS cnt RETURN {name: c.name, count: cnt} AS row ORDER BY cnt DESC LIMIT 10
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
                "No results found. This could mean:\n" +
                "- No data matches your query criteria\n" +
                "- The relationships you're querying don't exist in the database\n" +
                "- Try a simpler query first to verify the data exists"
            } else {
                "Found ${results.size} rows:\n" + results.joinToString("\n") { row ->
                    row.entries.joinToString(", ") { (k, v) -> "$k: $v" }
                }
            }
        } catch (e: Exception) {
            val errorMessage = e.message ?: "Unknown error"
            logger.error("Error executing rows query: {}", errorMessage, e)

            val guidance = when {
                errorMessage.contains("Invalid input") ->
                    "Cypher syntax error. Check your query syntax."
                errorMessage.contains("not defined") || errorMessage.contains("unknown") ->
                    "Unknown label or relationship type. Check that you're using types from the schema."
                else -> "Please fix the query and try again."
            }

            """
                ERROR: $errorMessage
                $guidance
            """.trimIndent()
        }
    }

    private fun executeForCount(query: CompiledCypherQuery): Int {
        // First try direct integer result (for proper count queries)
        try {
            return persistenceManager.getOne(
                QuerySpecification
                    .withStatement(query.query)
                    .transform(Int::class.java)
            )
        } catch (e: Exception) {
            // If that fails, try to extract 'count' from a map result
            @Suppress("UNCHECKED_CAST")
            val results = persistenceManager.query(
                QuerySpecification
                    .withStatement(query.query)
                    .transform(Map::class.java)
            ) as List<Map<String, Any>>

            if (results.size == 1) {
                val row = results[0]
                // Try to find a count-like value
                val countValue = row["count"] ?: row["cnt"] ?: row.values.firstOrNull()
                if (countValue is Number) {
                    return countValue.toInt()
                }
            }
            // Re-throw original exception if we couldn't recover
            throw e
        }
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
