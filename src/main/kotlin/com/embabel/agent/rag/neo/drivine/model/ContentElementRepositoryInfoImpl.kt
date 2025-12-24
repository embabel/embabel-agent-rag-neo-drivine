package com.embabel.agent.rag.neo.drivine.model

import com.embabel.agent.rag.store.ContentElementRepositoryInfo

data class ContentElementRepositoryInfoImpl(
    override val chunkCount: Int,
    override val documentCount: Int,
    override val contentElementCount: Int,
    override val hasEmbeddings: Boolean = true,
    override val isPersistent: Boolean = true
) : ContentElementRepositoryInfo
