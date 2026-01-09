// Create HAS_PARENT relationships for all ContentElements (backward compatibility)
// This runs as a single batch operation
MATCH (child:ContentElement) WHERE child.parentId IS NOT NULL
WITH child
MATCH (parent:ContentElement {id: child.parentId})
MERGE (child)-[:HAS_PARENT]->(parent)
