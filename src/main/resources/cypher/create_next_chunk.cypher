// Create NEXT_CHUNK relationship between two consecutive chunks.
// Parameters:
//   currentChunkId: The ID of the current chunk
//   nextChunkId: The ID of the next chunk

MATCH (current:Chunk {id: $currentChunkId})
MATCH (next:Chunk {id: $nextChunkId})
MERGE (current)-[:NEXT_CHUNK]->(next)
