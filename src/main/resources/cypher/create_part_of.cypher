// Create PART_OF relationship from Chunk to its parent.
// Aligns with Neo4j LLM Graph Builder convention.
// Parameters:
//   chunkId: The ID of the chunk
//   parentId: The ID of the parent (Document, Section, or LeafSection)

MATCH (chunk:Chunk {id: $chunkId})
MATCH (parent:ContentElement {id: $parentId})
MERGE (chunk)-[:PART_OF]->(parent)
