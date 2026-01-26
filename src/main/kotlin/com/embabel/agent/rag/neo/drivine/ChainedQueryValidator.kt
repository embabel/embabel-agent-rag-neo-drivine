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
