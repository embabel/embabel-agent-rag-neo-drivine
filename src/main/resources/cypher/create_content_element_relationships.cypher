// Create HAS_PARENT and PART_OF relationships in a single statement
// HAS_PARENT: backward compatibility for all ContentElements
// PART_OF: Neo4j LLM Graph Builder convention for Chunks to Documents

CALL {
    MATCH (child:ContentElement) WHERE child.parentId IS NOT NULL
    WITH child
    MATCH (parent:ContentElement {id: child.parentId})
    MERGE (child)-[:HAS_PARENT]->(parent)
}

CALL {
    MATCH (chunk:Chunk) WHERE chunk.parentId IS NOT NULL
    WITH chunk
    MATCH (parent {id: chunk.parentId})
    WHERE 'Document' IN labels(parent) OR 'ContentRoot' IN labels(parent)
    MERGE (chunk)-[:PART_OF]->(parent)
}
