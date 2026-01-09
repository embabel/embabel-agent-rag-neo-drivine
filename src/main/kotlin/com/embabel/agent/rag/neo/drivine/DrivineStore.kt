package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.api.common.Embedding
import com.embabel.agent.rag.filter.PropertyFilter
import com.embabel.agent.rag.ingestion.RetrievableEnhancer
import com.embabel.agent.rag.model.*
import com.embabel.agent.rag.neo.drivine.mappers.DefaultContentElementRowMapper
import com.embabel.agent.rag.neo.drivine.model.ContentElementRepositoryInfoImpl
import com.embabel.agent.rag.service.*
import com.embabel.agent.rag.service.EntitySearch
import com.embabel.agent.rag.service.RagRequest
import com.embabel.agent.rag.service.ResultExpander
import com.embabel.agent.rag.service.support.FunctionRagFacet
import com.embabel.agent.rag.service.support.RagFacet
import com.embabel.agent.rag.service.support.RagFacetProvider
import com.embabel.agent.rag.service.support.RagFacetResults
import com.embabel.agent.rag.store.AbstractChunkingContentElementRepository
import com.embabel.agent.rag.store.ChunkingContentElementRepository
import com.embabel.agent.rag.store.ContentElementRepositoryInfo
import com.embabel.agent.rag.store.DocumentDeletionResult
import com.embabel.common.ai.model.DefaultModelSelectionCriteria
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.ai.model.ModelProvider
import com.embabel.common.core.types.SimilarityCutoff
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.TextSimilaritySearchRequest
import org.drivine.manager.PersistenceManager
import org.drivine.mapper.RowMapper
import org.drivine.query.QuerySpecification
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate

class DrivineStore @JvmOverloads constructor(
    val persistenceManager: PersistenceManager,
    val properties: NeoRagServiceProperties,
    embeddingService: EmbeddingService,
    platformTransactionManager: PlatformTransactionManager,
    private val cypherSearch: CypherSearch,
    override val enhancers: List<RetrievableEnhancer> = emptyList(),
    private val contentElementMapper: RowMapper<ContentElement> = DefaultContentElementRowMapper(),
    private val chunkFilterConverter: CypherFilterConverter = CypherFilterConverter(nodeAlias = "chunk"),
) : AbstractChunkingContentElementRepository(properties, embeddingService), ChunkingContentElementRepository, RagFacetProvider,
    CoreSearchOperations, FilteringVectorSearch, FilteringTextSearch, ResultExpander {

    override val name get() = properties.name

    override val luceneSyntaxNotes = "Full support"

    override fun supportsType(type: String): Boolean {
        return type == Chunk::class.java.simpleName
    }

    override fun provision() {
        logger.info("Provisioning with properties {}", properties)
        // TODO do we want this on ContentElement?
        createVectorIndex(properties.contentElementIndex, "Chunk")
        createVectorIndex(properties.entityIndex, properties.entityNodeName)
        createFullTextIndex(properties.contentElementFullTextIndex, "Chunk", listOf("text"))
        createFullTextIndex(properties.entityFullTextIndex, properties.entityNodeName, listOf("name", "description"))
        logger.info("Provisioning complete")
    }

    override fun commit() {
        // TODO may need to do this?
    }

    override fun createInternalRelationships(root: NavigableDocument) {
        // Create HAS_PARENT and PART_OF relationships via batch Cypher
        cypherSearch.query(
            purpose = "Create content element relationships",
            query = "create_content_element_relationships",
            params = emptyMap<String, Any>()
        )

        // Get all chunks from the document in order and group by parentId
        val chunks = root.descendants().filterIsInstance<Chunk>().toList()
        val chunksByParent = chunks.groupBy { it.parentId }

        // Create FIRST_CHUNK and NEXT_CHUNK relationships for each parent
        for ((parentId, parentChunks) in chunksByParent) {
            if (parentChunks.isEmpty()) continue

            val chunkIds = parentChunks.map { it.id }
            cypherSearch.query(
                purpose = "Create chunk linked list for parent $parentId",
                query = "create_chunk_linked_list",
                params = mapOf(
                    "parentId" to parentId,
                    "chunkIds" to chunkIds
                )
            )
        }

        logger.info(
            "Created content hierarchy relationships for {} chunks across {} parents",
            chunks.size,
            chunksByParent.size
        )
    }

    override fun deleteRootAndDescendants(uri: String): DocumentDeletionResult? {
        logger.info("Deleting document with URI: {}", uri)

        try {
            val result = cypherSearch.query(
                "Delete document and descendants",
                query = "delete_document_and_descendants",
                params = mapOf("uri" to uri)
            )

            val deletedCount = result.numberOrZero<Int>("deletedCount")

            if (deletedCount == 0) {
                logger.warn("No document found with URI: {}", uri)
                return null
            }

            logger.info("Deleted {} elements for document with URI: {}", deletedCount, uri)
            return DocumentDeletionResult(
                rootUri = uri,
                deletedCount = deletedCount
            )
        } catch (e: Exception) {
            logger.error("Error deleting document with URI: {}", uri, e)
            throw e
        }
    }

    override fun existsRootWithUri(uri: String): Boolean {
        val statement =
            "MATCH(c) WHERE c.uri = \$uri AND ('Document' IN labels(c) OR 'ContentRoot' IN labels(c)) RETURN COUNT(c) AS count"
        val parameters = mapOf("uri" to uri)
        return try {
            val count = cypherSearch.queryForInt(statement, parameters)
            count > 0
        } catch (e: Exception) {
            logger.error("Error checking existence of root with URI: {}", uri, e)
            false
        }
    }

    override fun findContentRootByUri(uri: String): ContentRoot? {
        logger.debug("Finding root document with URI: {}", uri)

        val statement = cypherContentElementQuery(
            " WHERE c.uri = \$uri AND ('Document' IN labels(c) OR 'ContentRoot' IN labels(c)) "
        )
        val parameters = mapOf("uri" to uri)
        try {
            val spec = QuerySpecification
                .withStatement(statement)
                .bind(parameters)
                .mapWith(contentElementMapper)

            val result = persistenceManager.maybeGetOne(spec)
            logger.debug("Root document with URI {} found: {}", uri, result != null)
            return result as? ContentRoot
        } catch (e: Exception) {
            logger.error("Error finding root with URI: {}", uri, e)
            return null
        }
    }

    override fun persistChunksWithEmbeddings(
        chunks: List<Chunk>,
        embeddings: Map<String, FloatArray>
    ) {
        //TODO: Fix the !! below
        chunks.forEach { chunk -> embedRetrievable(chunk, embeddings[chunk.id]!!) }
    }

    fun embeddingFor(text: String): Embedding =
        embeddingService!!.embed(text)

    private fun embedRetrievable(
        retrievable: Retrievable,
        embedding: Embedding,
    ) {
        try {
            val cypher = """
                MERGE (n:${retrievable.labels().joinToString(":")} {id: ${'$'}id})
                SET n.embedding = ${'$'}embedding,
                 n.embeddingModel = ${'$'}embeddingModel,
                 n.embeddedAt = timestamp()
                RETURN {nodesUpdated: COUNT(n) }
               """.trimIndent()
            val params = mapOf(
                "id" to retrievable.id,
                "embedding" to embedding,
                "embeddingModel" to embeddingService!!.name,
            )
            val result = cypherSearch.query(
                purpose = "embedding",
                query = cypher,
                params = params,
            )
            val nodesUpdated = result.numberOrZero<Int>("nodesUpdated")
            if (nodesUpdated == 0) {
                logger.warn(
                    "Expected to set embedding properties, but set 0. chunkId={}, cypher={}",
                    retrievable.id,
                    cypher,
                )
            }
        } catch (e: Exception) {
            logger.warn(
                "Unable to generate embedding for retrievable id='{}': {}",
                retrievable.id,
                e.message,
                e,
            )
        }
    }

    override fun expandResult(
        id: String,
        method: ResultExpander.Method,
        elementsToAdd: Int
    ): List<ContentElement> {
        TODO("Not yet implemented")
    }

    override fun info(): ContentElementRepositoryInfo {
        val statement = """
            CALL {
              MATCH (c:Chunk)
              RETURN count(c) AS chunkCount
            }
            CALL {
              MATCH (d:Document)
              RETURN count(d) AS documentCount
            }
            CALL {
              MATCH (e:ContentElement)
              RETURN count(e) AS contentElementCount
            }
            RETURN {
              chunkCount: chunkCount,
              documentCount: documentCount,
              contentElementCount: contentElementCount
            } AS stats
        """.trimIndent()

        val results = persistenceManager.getOne(
            QuerySpecification.withStatement(statement)
                .transform(ContentElementRepositoryInfoImpl::class.java)
        )
        return results
    }

    override fun findAllChunksById(chunkIds: List<String>): Iterable<Chunk> {
        val statement = cypherContentElementQuery(" WHERE c:Chunk AND c.id IN \$ids ")
        val spec = QuerySpecification
            .withStatement(statement)
            .bind(mapOf("ids" to chunkIds))
            .mapWith(contentElementMapper)
            .filterIsInstance<Chunk>()
        return persistenceManager.query(spec)
    }

    override fun findById(id: String): ContentElement? {
        val statement = cypherContentElementQuery(" WHERE c.id = \$id ")
        val spec = QuerySpecification
            .withStatement(statement)
            .bind(mapOf("id" to id))
            .mapWith(contentElementMapper)
        return persistenceManager.maybeGetOne(spec)
    }

    override fun findChunksForEntity(entityId: String): List<Chunk> {
        val statement = """
            MATCH (e:Entity {id: ${'$'}entityId})<-[:HAS_ENTITY]-(chunk:Chunk)
            RETURN properties(chunk)
            """.trimIndent()
        val spec = QuerySpecification
            .withStatement(statement)
            .bind(mapOf("entityId" to entityId))
            .transform(Map::class.java)
            .map({
                Chunk(
                    id = it["id"] as String,
                    text = it["text"] as String,
                    parentId = it["parentId"] as String,
                    metadata = emptyMap(), //TODO Can it ever be populated?
                )
            })
        return persistenceManager.query(spec)
    }

    override fun save(element: ContentElement): ContentElement {
        cypherSearch.query(
            "Save element",
            query = "save_content_element",
            params = mapOf(
                "id" to element.id,
                "labels" to element.labels(),
                "properties" to element.propertiesToPersist(),
            )
        )
        return element
    }

    fun search(ragRequest: RagRequest): RagFacetResults<Retrievable> {
        val embedding = embeddingFor(ragRequest.query)
        val allResults = mutableListOf<SimilarityResult<out Retrievable>>()
        if (ragRequest.contentElementSearch.types.contains(Chunk::class.java)) {
            allResults += safelyExecuteInTransaction { chunkSearch(ragRequest, embedding) }
        } else {
            logger.info("No chunk search specified, skipping chunk search")
        }

        if (ragRequest.entitySearch != null) {
            allResults += safelyExecuteInTransaction { entitySearch(ragRequest, embedding) }
        } else {
            logger.info("No entity search specified, skipping entity search")
        }

        // TODO should reward multiple matches
        val mergedResults: List<SimilarityResult<out Retrievable>> = allResults
            .distinctBy { it.match.id }
            .sortedByDescending { it.score }
            .take(ragRequest.topK)
        return RagFacetResults(
            facetName = this.name,
            results = mergedResults,
        )
    }

    override fun facets(): List<RagFacet<out Retrievable>> {
        return listOf(
            FunctionRagFacet(
                name = "DrivineRagService",
                searchFunction = ::search,
            )
        )
    }

    private fun cypherContentElementQuery(whereClause: String): String =
        """
            MATCH (c:ContentElement)
            $whereClause
            RETURN
              {
                id: c.id,
                uri: c.uri,
                text: c.text,
                parentId: c.parentId,
                ingestionDate: c.ingestionTimestamp,
                labels: labels(c),
                properties: properties(c)
              } AS result
            """.trimIndent()

    private val readonlyTransactionTemplate = TransactionTemplate(platformTransactionManager).apply {
        isReadOnly = true
        propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRED
    }

    private fun safelyExecuteInTransaction(block: () -> List<SimilarityResult<out Retrievable>>): List<SimilarityResult<out Retrievable>> {
        return try {
            readonlyTransactionTemplate.execute { block() } as List<SimilarityResult<out Retrievable>>
        } catch (e: Exception) {
            logger.error("Error during RAG search transaction", e)
            emptyList()
        }
    }

    private fun chunkSearch(
        ragRequest: RagRequest,
        embedding: Embedding,
    ): List<SimilarityResult<out Chunk>> {
        val chunkSimilarityResults = chunkSimilaritySearch(ragRequest, embedding)
        val chunkFullTextResults = chunkFullTextSearch(ragRequest)
        return chunkSimilarityResults + chunkFullTextResults
    }

    override fun <T : Retrievable> vectorSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("DrivineStore vectorSearch only supports Chunk class, got: $clazz")
        }
        @Suppress("UNCHECKED_CAST")
        return chunkSimilaritySearch(
            request,
            embeddingFor(request.query),
        ) as List<SimilarityResult<T>>
    }

    private fun chunkSimilaritySearch(
        request: TextSimilaritySearchRequest,
        embedding: Embedding,
    ): List<SimilarityResult<Chunk>> {
        val results = cypherSearch.chunkSimilaritySearch(
            "Chunk similarity search",
            query = "chunk_vector_search",
            params = commonParameters(request) + mapOf(
                "vectorIndex" to properties.contentElementIndex,
                "queryVector" to embedding,
            ),
            logger = logger,
        )
        logger.info("{} chunk similarity results for query '{}'", results.size, request.query)
        return results
    }

    internal fun chunkFullTextSearch(
        request: TextSimilaritySearchRequest,
    ): List<SimilarityResult<out Chunk>> {
        val results = cypherSearch.chunkFullTextSearch(
            purpose = "Chunk full text search",
            query = "chunk_fulltext_search",
            params = commonParameters(request) + mapOf(
                "fulltextIndex" to properties.contentElementFullTextIndex,
                "searchText" to "\"${request.query}\"",
            ),
            logger = logger,
        )
        logger.info("{} chunk full-text results for query '{}'", results.size, request.query)
        return results
    }

    override fun <T : Retrievable> textSearch(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>
    ): List<SimilarityResult<T>> {
        val results = cypherSearch.chunkFullTextSearch(
            purpose = "Chunk full text search",
            query = "chunk_fulltext_search",
            params = commonParameters(request) + mapOf(
                "fulltextIndex" to properties.contentElementFullTextIndex,
                "searchText" to request.query,
            ),
            logger = logger,
        )
        @Suppress("UNCHECKED_CAST")
        return results as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> vectorSearchWithFilter(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>,
        metadataFilter: PropertyFilter?,
        propertyFilter: PropertyFilter?,
    ): List<SimilarityResult<T>> {
        if (clazz != Chunk::class.java) {
            throw IllegalArgumentException("DrivineStore vectorSearchWithFilter only supports Chunk class, got: $clazz")
        }
        // In Neo4j, chunk metadata is stored as node properties, so both filters can use native Cypher
        val filterResult = combineFilters(metadataFilter, propertyFilter)
        logger.info(
            "Performing vector search with filter: query='{}', topK={}, metadataFilter={}, propertyFilter={}",
            request.query, request.topK, metadataFilter, propertyFilter
        )
        @Suppress("UNCHECKED_CAST")
        return chunkSimilaritySearchWithFilter(
            request,
            embeddingFor(request.query),
            filterResult,
        ) as List<SimilarityResult<T>>
    }

    override fun <T : Retrievable> textSearchWithFilter(
        request: TextSimilaritySearchRequest,
        clazz: Class<T>,
        metadataFilter: PropertyFilter?,
        propertyFilter: PropertyFilter?,
    ): List<SimilarityResult<T>> {
        // In Neo4j, chunk metadata is stored as node properties, so both filters can use native Cypher
        val filterResult = combineFilters(metadataFilter, propertyFilter)
        logger.info(
            "Performing text search with filter: query='{}', topK={}, metadataFilter={}, propertyFilter={}",
            request.query, request.topK, metadataFilter, propertyFilter
        )
        val results = cypherSearch.chunkFullTextSearchWithFilter(
            purpose = "Chunk full text search with filter",
            query = "chunk_fulltext_search",
            params = commonParameters(request) + mapOf(
                "fulltextIndex" to properties.contentElementFullTextIndex,
                "searchText" to request.query,
            ),
            filterResult = filterResult,
            logger = logger,
        )
        @Suppress("UNCHECKED_CAST")
        return results as List<SimilarityResult<T>>
    }

    /**
     * Combines metadata and property filters into a single CypherFilterResult.
     * In Neo4j, chunk metadata is stored as node properties (anything not in CORE_PROPERTIES),
     * so both filter types can be translated to native Cypher WHERE clauses.
     */
    private fun combineFilters(
        metadataFilter: PropertyFilter?,
        propertyFilter: PropertyFilter?,
    ): CypherFilterResult {
        val combinedFilter = when {
            metadataFilter != null && propertyFilter != null ->
                PropertyFilter.And(listOf(metadataFilter, propertyFilter))
            metadataFilter != null -> metadataFilter
            propertyFilter != null -> propertyFilter
            else -> null
        }
        return chunkFilterConverter.convert(combinedFilter)
    }

    private fun chunkSimilaritySearchWithFilter(
        request: TextSimilaritySearchRequest,
        embedding: Embedding,
        filterResult: CypherFilterResult,
    ): List<SimilarityResult<Chunk>> {
        val results = cypherSearch.chunkSimilaritySearchWithFilter(
            "Chunk similarity search with filter",
            query = "chunk_vector_search",
            params = commonParameters(request) + mapOf(
                "vectorIndex" to properties.contentElementIndex,
                "queryVector" to embedding,
            ),
            filterResult = filterResult,
            logger = logger,
        )
        logger.info("{} chunk similarity results for query '{}' with filter", results.size, request.query)
        return results
    }

    private fun entitySearch(
        ragRequest: RagRequest,
        embedding: FloatArray,
    ): List<SimilarityResult<out Retrievable>> {
        val allEntityResults = mutableListOf<SimilarityResult<out Retrievable>>()
        val labels = ragRequest.entitySearch?.labels ?: error("No entity search specified")
        val entityResults = entityVectorSearch(
            ragRequest,
            embedding,
            labels,
        )
        allEntityResults += entityResults
        logger.info("{} entity vector results for query '{}'", entityResults.size, ragRequest.query)
        val entityFullTextResults = cypherSearch.entityFullTextSearch(
            purpose = "Entity full text search",
            query = "entity_fulltext_search",
            params = commonParameters(ragRequest) + mapOf(
                "fulltextIndex" to properties.entityFullTextIndex,
                "searchText" to ragRequest.query,
                "labels" to labels,
            ),
            logger = logger,
        )
        logger.info("{} entity full-text results for query '{}'", entityFullTextResults.size, ragRequest.query)
        allEntityResults += entityFullTextResults

        if (ragRequest.entitySearch?.generateQueries == true) {
            val cypherResults =
                generateAndExecuteCypher(ragRequest, ragRequest.entitySearch!!).also { cypherResults ->
                    logger.info("{} Cypher results for query '{}'", cypherResults.size, ragRequest.query)
                }
            allEntityResults += cypherResults
        } else {
            logger.info("No query generation specified, skipping Cypher generation and execution")
        }
        logger.info("{} total entity results for query '{}'", entityFullTextResults.size, ragRequest.query)
        return allEntityResults
    }

    fun entityVectorSearch(
        request: SimilarityCutoff,
        embedding: FloatArray,
        labels: Set<String>,
    ): List<SimilarityResult<out EntityData>> {
        return cypherSearch.entityDataSimilaritySearch(
            purpose = "Mapped entity search",
            query = "entity_vector_search",
            params = commonParameters(request) + mapOf(
                "index" to properties.entityIndex,
                "queryVector" to embedding,
                "labels" to labels,
            ),
            logger,
        )
    }

    private fun generateAndExecuteCypher(
        request: RagRequest,
        entitySearch: EntitySearch,
    ): List<SimilarityResult<out Retrievable>> {
        TODO("Not yet implemented")
//        val schema = schemaResolver.getSchema(entitySearch)
//        if (schema == null) {
//            logger.info("No schema found for entity search {}, skipping Cypher execution", entitySearch)
//            return emptyList()
//        }
//
//        val cypherRagQueryGenerator = SchemaDrivenCypherRagQueryGenerator(
//            modelProvider,
//            schema,
//        )
//        val cypher = cypherRagQueryGenerator.generateQuery(request = request)
//        logger.info("Generated Cypher query: $cypher")
//
//        val cypherResults = readonlyTransactionTemplate.execute {
//            executeGeneratedCypher(cypher)
//        } ?: Result.failure(
//            IllegalStateException("Transaction failed or returned null while executing Cypher query: $cypher")
//        )
//        if (cypherResults.isSuccess) {
//            val results = cypherResults.getOrThrow()
//            if (results.isNotEmpty()) {
//                logger.info("Cypher query executed successfully, results: {}", results)
//                return results.map {
//                    // Most similar as we found them by a query
//                    SimpleSimilaritySearchResult(
//                        it,
//                        score = 1.0,
//                    )
//                }
//            }
//        }
//        return emptyList()
    }

//    /**
//     * Execute generate Cypher query, being sure to handle exceptions gracefully.
//     */
//    private fun executeGeneratedCypher(
//        query: CypherQuery,
//    ): Result<List<EntityData>> {
//        TODO("Not yet implemented")
//        try {
//            return Result.success(
//                ogmCypherSearch.queryForEntities(
//                    purpose = "cypherGeneratedQuery",
//                    query = query.query
//                )
//            )
//        } catch (e: Exception) {
//            logger.error("Error executing generated query: $query", e)
//            return Result.failure(e)
//        }
//    }

    private fun createVectorIndex(
        name: String,
        on: String,
    ) {
        val embeddingModel = embeddingService!!.model as EmbeddingModel // TODO: Fixme
        val statement = """
            CREATE VECTOR INDEX `$name` IF NOT EXISTS
            FOR (n:$on) ON (n.embedding)
            OPTIONS {indexConfig: {
            `vector.dimensions`: ${embeddingModel.dimensions()},
            `vector.similarity_function`: 'cosine'
            }}"""

        persistenceManager.execute(QuerySpecification.withStatement(statement))

    }

    private fun createFullTextIndex(
        name: String,
        on: String,
        properties: List<String>,
    ) {
        val propertiesString = properties.joinToString(", ") { "n.$it" }
        val statement = """|
                |CREATE FULLTEXT INDEX `$name` IF NOT EXISTS
                |FOR (n:$on) ON EACH [$propertiesString]
                |OPTIONS {
                |indexConfig: {
                |
                |   }
                |}
                """.trimMargin()
        persistenceManager.execute(QuerySpecification.withStatement(statement))
        logger.info("Created full-text index {} for {} on properties {}", name, on, properties)
    }


    private fun commonParameters(request: SimilarityCutoff) = mapOf(
        "topK" to request.topK,
        "similarityThreshold" to request.similarityThreshold,
    )


}
