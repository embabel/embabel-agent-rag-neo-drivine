package com.embabel.agent.rag.neo.drivine.mappers

import com.embabel.agent.rag.model.NamedEntityData
import com.embabel.agent.rag.model.SimpleNamedEntityData
import org.drivine.mapper.RowMapper

internal class NamedEntityDataRowMapper : RowMapper<NamedEntityData> {

    override fun map(row: Map<String, *>): NamedEntityData {
        @Suppress("UNCHECKED_CAST")
        return SimpleNamedEntityData(
            id = row["id"] as String,
            name = row["name"] as String,
            description = row["description"] as? String ?: "",
            labels = (row["labels"] as? List<*>)?.map { it.toString() }?.toSet() ?: emptySet(),
            properties = (row["properties"] as? Map<String, Any>) ?: emptyMap(),
        )
    }
}
