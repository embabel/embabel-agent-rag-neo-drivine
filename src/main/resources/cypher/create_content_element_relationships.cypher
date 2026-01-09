 // Create HAS_PARENT relationships for all ContentElements (backward compatibility)
MATCH (child:ContentElement) WHERE child.parentId IS NOT NULL
WITH child MATCH (parent:ContentElement {id: child.parentId})
MERGE (child)-[:HAS_PARENT]->(parent);

// Create PART_OF relationships for Chunks to their parent Documents
// Aligns with Neo4j LLM Graph Builder convention
MATCH (chunk:Chunk) WHERE chunk.parentId IS NOT NULL
WITH chunk MATCH (parent {id: chunk.parentId})
WHERE 'Document' IN labels(parent) OR 'ContentRoot' IN labels(parent)
MERGE (chunk)-[:PART_OF]->(parent);