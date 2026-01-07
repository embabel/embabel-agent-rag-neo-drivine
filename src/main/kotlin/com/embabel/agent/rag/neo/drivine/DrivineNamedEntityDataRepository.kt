package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.core.DataDictionary
import com.embabel.agent.rag.model.NamedEntity
import com.embabel.agent.rag.model.NamedEntityData
import com.embabel.agent.rag.neo.drivine.mappers.NamedEntityDataRowMapper
import com.embabel.agent.rag.neo.drivine.mappers.NamedEntityDataSimilarityMapper
import com.embabel.agent.rag.service.EntityIdentifier
import com.embabel.agent.rag.service.NamedEntityDataRepository
import com.embabel.agent.rag.service.RelationshipData
import com.embabel.common.ai.model.EmbeddingService
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.TextSimilaritySearchRequest
import com.embabel.common.util.loggerFor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.drivine.annotation.GraphView
import org.drivine.annotation.NodeFragment
import org.drivine.manager.GraphObjectManager
import org.drivine.manager.PersistenceManager
import org.drivine.mapper.RowMapper
import org.drivine.query.QuerySpecification

/**
 * Drivine-based implementation of [NamedEntityDataRepository] for Neo4j.
 *
 * Provides CRUD operations and search capabilities for [NamedEntityData] entities
 * stored in Neo4j. Queries nodes directly without fetching relationships.
 *
 * Supports:
 * - Basic CRUD: [save], [findById], [findByLabel], [delete]
 * - Full-text search via Neo4j fulltext indexes
 * - Vector similarity search via Neo4j vector indexes
 * - Relationship creation between entities using APOC
 *
 * ## Indexing
 * This repository relies on vector and full-text indexes created on nodes with the
 * [NeoRagServiceProperties.entityNodeName] label (default: "Entity"). These indexes
 * are provisioned by [DrivineStore.provision].
 *
 * When saving entities, this repository automatically ensures that
 * [NeoRagServiceProperties.entityNodeName] is included in the node's labels,
 * regardless of whether it was present in [NamedEntityData.labels]. This guarantees
 * that all saved entities are properly indexed for search operations.
 *
 * @param persistenceManager Drivine persistence manager for Neo4j operations
 * @param properties Configuration properties including index names and entity node label
 * @param embeddingService Service for generating embeddings for vector search
 * @param queryResolver Resolver for loading Cypher queries from external files
 * @param namedEntityDataMapper Row mapper for converting query results to [NamedEntityData]
 * @param namedEntityDataSimilarityMapper Row mapper for similarity search results
 * @param verifyIndexes If true (default), verifies required indexes exist at construction time
 *        and logs a warning if they are missing
 */
class DrivineNamedEntityDataRepository @JvmOverloads constructor(
    private val persistenceManager: PersistenceManager,
    private val properties: NeoRagServiceProperties,
    override val dataDictionary: DataDictionary,
    private val embeddingService: EmbeddingService,
    private val graphObjectManager: GraphObjectManager? = null,
    override val objectMapper: ObjectMapper = jacksonObjectMapper(),
    private val queryResolver: LogicalQueryResolver = FixedLocationLogicalQueryResolver(),
    private val namedEntityDataMapper: RowMapper<NamedEntityData> = NamedEntityDataRowMapper(),
    private val namedEntityDataSimilarityMapper: RowMapper<SimilarityResult<NamedEntityData>> = NamedEntityDataSimilarityMapper(),
    verifyIndexes: Boolean = true,
) : NamedEntityDataRepository {

    private val logger = loggerFor<DrivineNamedEntityDataRepository>()

    init {
        if (verifyIndexes) {
            verifyRequiredIndexes()
        }
    }

    private fun verifyRequiredIndexes() {
        val requiredIndexes = listOf(properties.entityIndex, properties.entityFullTextIndex)
        try {
            val statement = "SHOW INDEXES YIELD name RETURN collect(name) AS indexNames"
            @Suppress("UNCHECKED_CAST")
            val existingIndexes = persistenceManager.getOne(
                QuerySpecification
                    .withStatement(statement)
                    .transform(List::class.java)
            ) as List<String>

            val missingIndexes = requiredIndexes.filter { it !in existingIndexes }
            if (missingIndexes.isNotEmpty()) {
                logger.warn(
                    "Required indexes not found: {}. Run DrivineStore.provision() to create them. " +
                        "Search operations will fail until indexes are created.",
                    missingIndexes
                )
            }
        } catch (e: Exception) {
            logger.warn("Could not verify indexes: {}. Ensure indexes exist before using search operations.", e.message)
        }
    }

    override val luceneSyntaxNotes: String
        get() = "Full support"

    override fun createRelationship(
        a: EntityIdentifier,
        b: EntityIdentifier,
        relationship: RelationshipData
    ) {
        logger.debug(
            "Creating relationship: ({} {})-[:{}]->({} {})",
            a.type, a.id, relationship.name, b.type, b.id
        )
        val statement = resolveQuery("create_named_entity_relationship")
        val params = mapOf(
            "fromId" to a.id,
            "fromType" to a.type,
            "toId" to b.id,
            "toType" to b.type,
            "relType" to relationship.name,
            "relProperties" to relationship.properties,
        )

        persistenceManager.execute(
            QuerySpecification
                .withStatement(statement)
                .bind(params)
        )
        logger.debug("Created relationship: ({} {})-[:{}]->({} {})", a.type, a.id, relationship.name, b.type, b.id)
    }

    override fun delete(id: String): Boolean {
        logger.debug("Deleting entity with id: {}", id)
        val statement = """
            MATCH (e:${properties.entityNodeName} {id: ${'$'}id})
            DETACH DELETE e
            RETURN count(e) AS deletedCount
        """.trimIndent()

        val deletedCount = persistenceManager.getOne(
            QuerySpecification
                .withStatement(statement)
                .bind(mapOf("id" to id))
                .transform(Int::class.java)
        )
        val deleted = deletedCount > 0
        logger.debug("Delete entity id={}: deleted={}", id, deleted)
        return deleted
    }

    override fun findById(id: String): NamedEntityData? {
        logger.debug("Finding entity by id: {}", id)
        val statement = namedEntityQuery("e.id = \$id")
        return persistenceManager.maybeGetOne(
            QuerySpecification
                .withStatement(statement)
                .bind(mapOf("id" to id))
                .mapWith(namedEntityDataMapper)
        )
    }

    override fun isNativeType(type: Class<*>): Boolean =
        type.isAnnotationPresent(NodeFragment::class.java) ||
                type.isAnnotationPresent(GraphView::class.java)

    override fun <T : NamedEntity> findNativeAll(type: Class<T>): List<T>? {
        return graphObjectManager?.loadAll(type)
    }

    override fun <T : NamedEntity> findNativeById(
        id: String,
        type: Class<T>
    ): T? {
        return graphObjectManager?.load(id, type)
    }

    override fun findByLabel(label: String): List<NamedEntityData> {
        logger.debug("Finding entities by label: {}", label)
        val statement = namedEntityQuery("\$label IN labels(e)")
        return persistenceManager.query(
            QuerySpecification
                .withStatement(statement)
                .bind(mapOf("label" to label))
                .mapWith(namedEntityDataMapper)
        )
    }

    override fun save(entity: NamedEntityData): NamedEntityData {
        logger.debug("Saving entity: id={}, name={}", entity.id, entity.name)
        val labels = (entity.labels() + properties.entityNodeName).distinct()
        val labelsString = labels.joinToString(":")
        val statement = """
            MERGE (e:$labelsString {id: ${'$'}id})
            SET e.name = ${'$'}name,
                e.description = ${'$'}description,
                e.lastModifiedDate = timestamp()
            SET e += ${'$'}properties
            RETURN {
                id: e.id,
                name: e.name,
                description: e.description,
                labels: labels(e),
                properties: properties(e)
            } AS result
        """.trimIndent()

        val params = mapOf(
            "id" to entity.id,
            "name" to entity.name,
            "description" to entity.description,
            "properties" to entity.properties,
        )

        return persistenceManager.getOne(
            QuerySpecification
                .withStatement(statement)
                .bind(params)
                .mapWith(namedEntityDataMapper)
        )
    }

    override fun textSearch(request: TextSimilaritySearchRequest): List<SimilarityResult<NamedEntityData>> {
        logger.info("Executing text search: query='{}', topK={}", request.query, request.topK)
        val statement = resolveQuery("named_entity_fulltext_search")
        val params = mapOf(
            "fulltextIndex" to properties.entityFullTextIndex,
            "searchText" to request.query,
            "similarityThreshold" to request.similarityThreshold,
            "topK" to request.topK,
            "entityNodeName" to properties.entityNodeName,
        )

        val results = persistenceManager.query(
            QuerySpecification
                .withStatement(statement)
                .bind(params)
                .mapWith(namedEntityDataSimilarityMapper)
        )
        logger.info("{} text search results for query '{}'", results.size, request.query)
        return results
    }

    override fun vectorSearch(request: TextSimilaritySearchRequest): List<SimilarityResult<NamedEntityData>> {
        val embedding = embeddingService.embed(request.query)
        logger.info("Executing vector search: query='{}', topK={}", request.query, request.topK)
        val statement = resolveQuery("named_entity_vector_search")
        val params = mapOf(
            "vectorIndex" to properties.entityIndex,
            "queryVector" to embedding,
            "similarityThreshold" to request.similarityThreshold,
            "topK" to request.topK,
            "entityNodeName" to properties.entityNodeName,
        )

        val results = persistenceManager.query(
            QuerySpecification
                .withStatement(statement)
                .bind(params)
                .mapWith(namedEntityDataSimilarityMapper)
        )
        logger.info("{} vector search results for query '{}'", results.size, request.query)
        return results
    }

    private fun namedEntityQuery(whereClause: String): String =
        """
        MATCH (e:${properties.entityNodeName})
        WHERE $whereClause
        RETURN {
            id: e.id,
            name: COALESCE(e.name, ''),
            description: COALESCE(e.description, ''),
            labels: labels(e),
            properties: properties(e)
        } AS result
        """.trimIndent()

    private fun resolveQuery(name: String): String =
        queryResolver.resolve(name) ?: error("Could not resolve query: $name")
}
