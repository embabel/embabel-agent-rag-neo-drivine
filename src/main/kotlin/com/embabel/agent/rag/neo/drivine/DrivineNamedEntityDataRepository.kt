package com.embabel.agent.rag.neo.drivine

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
 * @param persistenceManager Drivine persistence manager for Neo4j operations
 * @param properties Configuration properties including index names and entity node label
 * @param embeddingService Service for generating embeddings for vector search
 * @param namedEntityDataMapper Row mapper for converting query results to [NamedEntityData]
 * @param namedEntityDataSimilarityMapper Row mapper for similarity search results
 */
class DrivineNamedEntityDataRepository @JvmOverloads constructor(
    private val persistenceManager: PersistenceManager,
    private val properties: NeoRagServiceProperties,
    private val embeddingService: EmbeddingService,
    private val namedEntityDataMapper: RowMapper<NamedEntityData> = NamedEntityDataRowMapper(),
    private val namedEntityDataSimilarityMapper: RowMapper<SimilarityResult<NamedEntityData>> = NamedEntityDataSimilarityMapper(),
) : NamedEntityDataRepository {

    private val logger = loggerFor<DrivineNamedEntityDataRepository>()

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
        val statement = """
            MATCH (from:${properties.entityNodeName} {id: ${'$'}fromId})
            WHERE ${'$'}fromType IN labels(from)
            MATCH (to:${properties.entityNodeName} {id: ${'$'}toId})
            WHERE ${'$'}toType IN labels(to)
            CALL apoc.create.relationship(from, ${'$'}relType, ${'$'}relProperties, to)
            YIELD rel
            RETURN type(rel) AS relationType
        """.trimIndent()

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
        val labelsString = entity.labels().joinToString(":")
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
        val statement = """
            CALL db.index.fulltext.queryNodes(${'$'}fulltextIndex, ${'$'}searchText)
            YIELD node AS e, score
            WHERE score IS NOT NULL AND '${properties.entityNodeName}' IN labels(e)
            WITH collect({node: e, score: score}) AS results, max(score) AS maxScore
            WHERE maxScore IS NOT NULL AND maxScore > 0
            UNWIND results AS result
            WITH result.node AS e,
                 COALESCE(result.score / maxScore, 0.0) AS normalizedScore
            WHERE normalizedScore >= ${'$'}similarityThreshold
            RETURN {
                id: e.id,
                name: COALESCE(e.name, ''),
                description: COALESCE(e.description, ''),
                labels: labels(e),
                properties: properties(e),
                score: normalizedScore
            } AS result
            ORDER BY result.score DESC
            LIMIT ${'$'}topK
        """.trimIndent()

        val params = mapOf(
            "fulltextIndex" to properties.entityFullTextIndex,
            "searchText" to request.query,
            "similarityThreshold" to request.similarityThreshold,
            "topK" to request.topK,
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
        val statement = """
            CALL db.index.vector.queryNodes(${'$'}vectorIndex, ${'$'}topK, ${'$'}queryVector)
            YIELD node AS e, score
            WHERE score >= ${'$'}similarityThreshold
              AND '${properties.entityNodeName}' IN labels(e)
            RETURN {
                id: COALESCE(e.id, ''),
                name: COALESCE(e.name, ''),
                description: COALESCE(e.description, ''),
                labels: labels(e),
                properties: properties(e),
                score: score
            } AS result
            ORDER BY result.score DESC
        """.trimIndent()

        val params = mapOf(
            "vectorIndex" to properties.entityIndex,
            "queryVector" to embedding,
            "similarityThreshold" to request.similarityThreshold,
            "topK" to request.topK,
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
}
