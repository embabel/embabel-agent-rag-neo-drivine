CALL db.index.fulltext.queryNodes($fulltextIndex, $searchText)
YIELD node AS n, score
WHERE score IS NOT NULL AND $entityNodeName IN labels(n)
WITH collect({node: n, score: score}) AS results, max(score) AS maxScore
WHERE maxScore IS NOT NULL AND maxScore > 0
UNWIND results AS result
WITH result.node AS n,
     COALESCE(result.score / maxScore, 0.0) AS normalizedScore
WHERE normalizedScore >= $similarityThreshold
RETURN {
    id: n.id,
    name: COALESCE(n.name, ''),
    description: COALESCE(n.description, ''),
    labels: labels(n),
    properties: properties(n),
    score: normalizedScore
} AS result
ORDER BY result.score DESC
LIMIT $topK
