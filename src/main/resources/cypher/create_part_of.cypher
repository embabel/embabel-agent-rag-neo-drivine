// Create PART_OF relationship from Chunk to its parent Document.
// Aligns with Neo4j LLM Graph Builder convention.
// Parameters:
//   chunkId: The ID of the chunk
//   parentId: The ID of the parent document

MATCH (chunk:Chunk {id: $chunkId})
MATCH (parent {id: $parentId})
WHERE 'Document' IN labels(parent) OR 'ContentRoot' IN labels(parent)
MERGE (chunk)-[:PART_OF]->(parent)
RETURN count(*) AS created
