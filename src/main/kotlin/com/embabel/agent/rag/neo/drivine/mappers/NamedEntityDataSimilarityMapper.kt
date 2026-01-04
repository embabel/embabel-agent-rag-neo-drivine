package com.embabel.agent.rag.neo.drivine.mappers

import com.embabel.agent.rag.model.NamedEntityData
import com.embabel.agent.rag.model.SimpleNamedEntityData
import com.embabel.common.core.types.SimilarityResult
import com.embabel.common.core.types.SimpleSimilaritySearchResult
import org.drivine.mapper.RowMapper

internal class NamedEntityDataSimilarityMapper : RowMapper<SimilarityResult<NamedEntityData>> {

    @Suppress("UNCHECKED_CAST")
    override fun map(row: Map<String, *>): SimilarityResult<NamedEntityData> {
        val match = SimpleNamedEntityData(
            id = row["id"] as String,
            name = row["name"] as String,
            description = row["description"] as? String ?: "",
            labels = (row["labels"] as? List<*>)?.map { it.toString() }?.toSet() ?: emptySet(),
            properties = (row["properties"] as? Map<String, Any>) ?: emptyMap(),
        )
        return SimpleSimilaritySearchResult(
            match = match,
            score = row["score"] as Double,
        )
    }
}
