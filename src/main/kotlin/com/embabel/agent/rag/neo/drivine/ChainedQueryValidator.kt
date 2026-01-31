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

import com.embabel.agent.core.DataDictionary

/**
 * A validator that chains multiple validators together and runs them in sequence.
 *
 * All validators in the chain are executed. If any validator throws an exception,
 * that exception propagates immediately and subsequent validators are not run.
 *
 * @param validators The validators to run in order
 */
class ChainedQueryValidator(
    private val validators: List<CypherQueryValidator>,
) : CypherQueryValidator {

    constructor(vararg validators: CypherQueryValidator) : this(validators.toList())

    override fun validate(query: CompiledCypherQuery, schema: DataDictionary) {
        validators.forEach { it.validate(query, schema) }
    }

}
