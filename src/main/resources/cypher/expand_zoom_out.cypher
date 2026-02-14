MATCH (child:ContentElement {id: $id})
WHERE child.parentId IS NOT NULL
MATCH (parent:ContentElement {id: child.parentId})
RETURN {
  id: parent.id,
  uri: parent.uri,
  text: parent.text,
  parentId: parent.parentId,
  ingestionDate: parent.ingestionTimestamp,
  labels: labels(parent),
  properties: properties(parent)
} AS result