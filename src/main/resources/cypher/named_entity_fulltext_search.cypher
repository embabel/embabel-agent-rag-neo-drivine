CALL db.index.fulltext.queryNodes($fulltextIndex, $searchText)
YIELD node AS e, score
WHERE score IS NOT NULL AND $entityNodeName IN labels(e)
WITH collect({node: e, score: score}) AS results, max(score) AS maxScore
WHERE maxScore IS NOT NULL AND maxScore > 0
UNWIND results AS result
WITH result.node AS e,
     COALESCE(result.score / maxScore, 0.0) AS normalizedScore
WHERE normalizedScore >= $similarityThreshold
RETURN {
    id: e.id,
    name: COALESCE(e.name, ''),
    description: COALESCE(e.description, ''),
    labels: labels(e),
    properties: properties(e),
    score: normalizedScore
} AS result
ORDER BY result.score DESC
LIMIT $topK
