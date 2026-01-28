MATCH (from {id: $fromId})
WHERE $fromType IN labels(from)
MATCH (to {id: $toId})
WHERE $toType IN labels(to)
CALL apoc.merge.relationship(from, $relType, {}, $relProperties, to, $relProperties)
YIELD rel
RETURN type(rel) AS relationType
