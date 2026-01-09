// Create FIRST_CHUNK relationship from parent to first chunk.
// Parameters:
//   parentId: The ID of the parent (Document, Section, or LeafSection)
//   firstChunkId: The ID of the first chunk

MATCH (parent:ContentElement {id: $parentId})
MATCH (firstChunk:Chunk {id: $firstChunkId})
MERGE (parent)-[:FIRST_CHUNK]->(firstChunk)
