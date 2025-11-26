# Embabel RAG Neo4j Drivine

Neo4j RAG (Retrieval-Augmented Generation) implementation using Drivine for the Embabel Agent framework.

## Overview

This module provides a Neo4j-based implementation of the RAG pattern using Drivine4j, a lightweight Neo4j driver for Java/Kotlin. It includes:

- **CypherSearch**: Interface for executing Cypher queries and retrieving results
- **DrivineCypherSearch**: Main implementation using Drivine for Neo4j operations
- **DrivineStore**: Content element repository for storing and retrieving documents and chunks
- **LogicalQueryResolver**: Resolves logical query names to Cypher query files
- **Mappers**: Row mappers for converting Neo4j query results to domain objects
  - `ContentElementMapper`: Maps to ContentElement (Document/Chunk)
  - `EntityDataMapper`: Maps to EntityData
  - `EntityDataSimilarityMapper`: Maps to similarity search results for entities
  - `ChunkSimilarityMapper`: Maps to similarity search results for chunks

## Dependencies

- **Drivine4j**: Lightweight Neo4j driver
- **Embabel Agent RAG Pipeline**: Core RAG abstractions and interfaces
- **Spring Boot**: For dependency injection and transaction management
- **Kotlin**: Implementation language

## Usage

Add this dependency to your project:

```xml
<dependency>
    <groupId>com.embabel.agent</groupId>
    <artifactId>embabel-rag-neo-drivine</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Configuration

Configure Neo4j connection properties in your application configuration:

```yaml
neo-rag-service:
  cypher-directory: classpath:cypher
```

## Cypher Queries

Cypher query files are located in `src/main/resources/cypher/`:

- `chunk_vector_search.cypher`: Vector similarity search for chunks
- `entity_vector_search.cypher`: Vector similarity search for entities
- `chunk_fulltext_search.cypher`: Full-text search for chunks
- `entity_fulltext_search.cypher`: Full-text search for entities
- `vector_cluster.cypher`: Clustering based on vector similarity
- `save_content_element.cypher`: Save documents and chunks
- `create_entity.cypher`: Create named entities
- `delete_document_and_descendants.cypher`: Delete documents and their chunks
- And more...

## Testing

The module includes integration tests using Testcontainers for Neo4j:

```bash
mvn test
```

## License

See LICENSE file in the root directory.
