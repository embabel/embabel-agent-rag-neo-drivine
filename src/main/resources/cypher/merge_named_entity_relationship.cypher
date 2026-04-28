MATCH (from {id: $fromId})
WHERE $fromType IN labels(from)
MATCH (to {id: $toId})
WHERE $toType IN labels(to)
MERGE (from)-[r:$($relType)]->(to)
$($setClause)
RETURN type(r) AS relationType