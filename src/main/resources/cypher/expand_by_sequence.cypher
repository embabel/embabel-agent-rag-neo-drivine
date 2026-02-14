MATCH (c:ContentElement:Chunk)
WHERE c.container_section_id = $containerSectionId
  AND c.sequence_number >= $minSeq
  AND c.sequence_number <= $maxSeq
RETURN {
  id: c.id,
  uri: c.uri,
  text: c.text,
  parentId: c.parentId,
  ingestionDate: c.ingestionTimestamp,
  labels: labels(c),
  properties: properties(c)
} AS result
ORDER BY c.sequence_number