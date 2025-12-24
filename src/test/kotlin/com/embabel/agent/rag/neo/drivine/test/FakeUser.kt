/*
 * Copyright 2024-2025 Embabel Software, Inc.
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
package com.embabel.agent.rag.neo.drivine.test

/**
 * Simple fake user for testing purposes.
 * Mirrors the structure of GuideUserData from the guide app.
 */
data class FakeUser(
    val id: String = "test-user-001",
    val displayName: String = "Test User",
    val persona: String? = "adaptive",
    val customPrompt: String? = null
) {
    companion object {
        /**
         * Default test user instance.
         */
        val DEFAULT = FakeUser()
    }
}