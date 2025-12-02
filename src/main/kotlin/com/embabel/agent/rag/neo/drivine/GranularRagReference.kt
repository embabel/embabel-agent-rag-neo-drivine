package com.embabel.agent.rag.neo.drivine

import com.embabel.agent.api.common.LlmReference
import com.embabel.agent.rag.service.RagRequest
import com.embabel.agent.rag.service.RagService
import com.embabel.agent.rag.service.SimilarityResults
import com.embabel.agent.rag.service.SimpleRagResponseFormatter
import com.embabel.common.core.types.ZeroToOne
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.ai.tool.annotation.Tool
import org.springframework.ai.tool.annotation.ToolParam

val DEFAULT_ACCEPTANCE = """
    Continue search until the question is answered, or you have to give up. 
    Be creative, try different types of queries. Generate HyDE queries if needed.
    You must be thorough and try different approaches.
    In this case report that you could not find the answer.
""".trimIndent()

class GranularRagReference @JvmOverloads constructor(
    override val name: String,
    override val description: String,
    val drivineStore: DrivineStore,
    val goal: String = DEFAULT_ACCEPTANCE,
    ) : LlmReference {

    private val toolInstance: Any = GranularTools(drivineStore)

    override fun toolInstance() = toolInstance

    override fun notes() = "Search acceptance criteria:\n$goal"

}

// TODO can we use repeatUntil?

// TODO make the interface generic so it isn't specify to drivine store

internal class GranularTools(
    private val drivineStore: DrivineStore,
) {
    private val logger: Logger = LoggerFactory.getLogger(GranularRagReference::class.java)

    // TODO do we want to check if this meets criteria before returning

    @Tool(description="Perform vector search. Specify topK and similarity threshold from 0-1")
    fun vectorSearch(query: String,
                     topK: Int,
                     @ToolParam(description ="similarity threshold from 0-1") threshold: ZeroToOne): String {
        logger.info("Performing vector search with query='{}', topK={}, threshold={}", query, topK, threshold)
        val embedding = drivineStore.embeddingFor(query)
        val results = drivineStore.chunkSimilaritySearch(RagRequest.query(query).withTopK(topK).withSimilarityThreshold(threshold), embedding)
        return SimpleRagResponseFormatter.formatResults(SimilarityResults.fromList(results))
    }

    @Tool(description="Perform BMI25 search. Specify topK and similarity threshold from 0-1")
    fun bmi25Search(query: String, topK: Int,
                    @ToolParam(description ="similarity threshold from 0-1") threshold: ZeroToOne): String {
        logger.info("Performing BMI25 search with query='{}', topK={}, threshold={}", query, topK, threshold)
        // TODO fix this
        return "BMI25 search not NOT IMPLEMENTED YET"
    }

    @Tool(description="Perform regex search across chunks. Specify topK")
    fun regexSearch(regex: String, topK: Int): String {
        logger.info("Performing regex search with regex='{}', topK={}", regex, topK)
        // TODO fix this
        return "Regex search not NOT IMPLEMENTED YET"
    }

    // entity search

    // get entity

    // expand entity

}