CALL db.index.vector.queryNodes($vectorIndex, $topK, $queryVector)
YIELD node AS e, score
WHERE score >= $similarityThreshold
  AND $entityNodeName IN labels(e)
RETURN {
    id: COALESCE(e.id, ''),
    name: COALESCE(e.name, ''),
    description: COALESCE(e.description, ''),
    labels: labels(e),
    properties: properties(e),
    score: score
} AS result
ORDER BY result.score DESC
