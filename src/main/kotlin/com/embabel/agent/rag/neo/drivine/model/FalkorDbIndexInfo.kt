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
package com.embabel.agent.rag.neo.drivine.model

/**
 * Metadata for a FalkorDB index, as returned by `CALL db.indexes()`.
 *
 * FalkorDB's `db.indexes()` yields: label, properties, types, options,
 * language, stopwords, entitytype, status, info.
 *
 * @see <a href="https://docs.falkordb.com/cypher/procedures.html">FalkorDB Procedures</a>
 */
data class FalkorDbIndexInfo(
    val label: String = "",
    val properties: List<String> = emptyList(),
    val types: Map<String, List<String>> = emptyMap(),
) {
    fun hasIndex(property: String, type: String): Boolean =
        properties.contains(property) &&
            types[property]?.any { it.equals(type, ignoreCase = true) } == true
}
