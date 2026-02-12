CALL db.index.vector.queryNodes($vectorIndex, $topK, $queryVector)
YIELD node AS n, score
WHERE score >= $similarityThreshold
  AND $entityNodeName IN labels(n)
RETURN {
    id: COALESCE(n.id, ''),
    name: COALESCE(n.name, ''),
    description: COALESCE(n.description, ''),
    labels: labels(n),
    properties: properties(n),
    score: score
} AS result
ORDER BY result.score DESC
