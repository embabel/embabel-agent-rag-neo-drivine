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
package com.embabel.agent.rag.graph

/**
 * Flattens a properties map into an explicit Cypher SET clause and the matching
 * bind parameters.
 *
 * Returns `"SET alias.k1 = $_p0, alias.k2 = $_p1"` paired with `{_p0=v1, _p1=v2}`,
 * or `("" to emptyMap())` when [properties] is empty. Keys containing characters
 * that are not valid bare Cypher identifiers (hyphen, space) are backtick-quoted.
 *
 * The `_p` bind-param prefix is reserved for this helper — callers must not bind
 * params under that name.
 *
 * TODO replace all callers with GraphObjectManager + @GraphView once the FalkorDB
 *   driver supports map-typed parameters (currently `SET n += $properties` fails).
 */
internal fun flattenToSetClause(
    nodeAlias: String,
    properties: Map<String, Any?>,
): Pair<String, Map<String, Any?>> {
    if (properties.isEmpty()) return "" to emptyMap()
    val bindParams = mutableMapOf<String, Any?>()
    val assignments = properties.entries.mapIndexed { i, (key, value) ->
        val paramName = "_p$i"
        bindParams[paramName] = value
        val quotedKey = if (key.any { !it.isLetterOrDigit() && it != '_' }) "`$key`" else key
        "$nodeAlias.$quotedKey = \$$paramName"
    }.joinToString(", ")
    return "SET $assignments" to bindParams
}
