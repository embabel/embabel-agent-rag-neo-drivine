// Create FIRST_CHUNK relationship from parent to first chunk.
// Parameters:
//   parentId: The ID of the parent document
//   firstChunkId: The ID of the first chunk

MATCH (parent {id: $parentId})
WHERE 'Document' IN labels(parent) OR 'ContentRoot' IN labels(parent)
MATCH (firstChunk:Chunk {id: $firstChunkId})
MERGE (parent)-[:FIRST_CHUNK]->(firstChunk)
RETURN count(*) AS created
