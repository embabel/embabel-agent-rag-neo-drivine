// Create FIRST_CHUNK and NEXT_CHUNK relationships for a sequence of chunks.
// Aligns with Neo4j LLM Graph Builder convention.
// Parameters:
//   parentId: The ID of the parent document
//   chunkIds: Ordered list of chunk IDs (first chunk first)
//
// Creates:
//   (Document)-[:FIRST_CHUNK]->(first Chunk)
//   (Chunk)-[:NEXT_CHUNK]->(next Chunk)

// Create FIRST_CHUNK relationship from parent to first chunk
CALL {
    WITH $parentId AS parentId, $chunkIds AS chunkIds
    MATCH (parent {id: parentId})
    WHERE ('Document' IN labels(parent) OR 'ContentRoot' IN labels(parent))
      AND size(chunkIds) > 0
    MATCH (firstChunk:Chunk {id: chunkIds[0]})
    MERGE (parent)-[:FIRST_CHUNK]->(firstChunk)
}

// Create NEXT_CHUNK relationships between consecutive chunks
CALL {
    WITH $chunkIds AS chunkIds
    WHERE size(chunkIds) > 1
    UNWIND range(0, size(chunkIds) - 2) AS i
    WITH chunkIds, i
    MATCH (current:Chunk {id: chunkIds[i]})
    MATCH (next:Chunk {id: chunkIds[i + 1]})
    MERGE (current)-[:NEXT_CHUNK]->(next)
}
