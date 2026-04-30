# Embabel RAG Graph

RAG (Retrieval-Augmented Generation) implementation for graph databases using Drivine, part of the Embabel Agent framework.

## Overview

This module provides a graph-database-backed implementation of the RAG pattern using Drivine4j. It supports **Neo4j**, **FalkorDB**, and **Memgraph** through a dialect abstraction that handles the Cypher differences between engines.

### Key Components

- **DrivineStore**: Content element repository for storing and retrieving documents, chunks, and embeddings
- **RagDialect**: Strategy interface for database-specific operations (index creation, vector search, fulltext search, embedding storage)
- **CypherSearch / DrivineCypherSearch**: Cypher query execution layer
- **LogicalQueryResolver**: Resolves logical query names to Cypher query files
- **Mappers**: Row mappers for converting query results to domain objects

### Supported Databases

| Feature | Neo4j | FalkorDB |
|---|---|---|
| Vector index creation | `CREATE VECTOR INDEX` | `CREATE VECTOR INDEX` |
| Vector search | `db.index.vector.queryNodes` | `db.idx.vector.queryNodes` + `vecf32()` |
| Fulltext index | `CREATE FULLTEXT INDEX` | `db.idx.fulltext.createNodeIndex` |
| Fulltext search | `db.index.fulltext.queryNodes` | `db.idx.fulltext.queryNodes` |
| Unique constraints | `CREATE CONSTRAINT` | `GRAPH.CONSTRAINT CREATE` (Redis) |
| Embedding storage | Node property | Node property |

## Dependencies

- **Drivine4j** (0.0.30+): Graph database driver with Neo4j and FalkorDB support
- **Embabel Agent RAG Pipeline**: Core RAG abstractions and interfaces
- **Spring Boot**: Dependency injection and transaction management
- **Kotlin**: Implementation language

## Usage

Add this dependency to your project:

```xml
<dependency>
    <groupId>com.embabel.agent</groupId>
    <artifactId>embabel-agent-rag-graph</artifactId>
    <version>0.2.0-SNAPSHOT</version>
</dependency>
```

### Selecting a Dialect

The dialect is resolved from Drivine's `DatabaseType`:

```kotlin
import com.embabel.agent.rag.graph.dialect.RagDialect
import org.drivine.connection.DatabaseType

val dialect = RagDialect.forDatabaseType(DatabaseType.FALKORDB)
```

Pass it when constructing `DrivineStore`:

```kotlin
DrivineStore(
    persistenceManager = persistenceManager,
    properties = properties,
    cypherSearch = cypherSearch,
    dialect = dialect,
    // ...
)
```

If no dialect is specified, `Neo4jRagDialect` is used by default.

## Configuration

Configure connection and RAG properties in your application configuration:

```yaml
database:
  datasources:
    graph:
      type: NEO4J          # or FALKORDB, MEMGRAPH
      host: localhost
      port: 7687
      user-name: neo4j
      password: secret
      database-name: neo4j

embabel:
  agent:
    rag:
      graph:
        content-element-index: embabel_content_index
        entity-index: embabel_entity_index
        content-element-full-text-index: embabel_content_fulltext_index
        entity-full-text-index: embabel_entity_fulltext_index
```

### FalkorDB Notes

- Unique constraints require the Redis command `GRAPH.CONSTRAINT CREATE`, which must be issued through the FalkorDB driver directly (not via Cypher).
- The `vecf32()` wrapper is required around vector parameters in search queries (handled by the dialect).
- Fulltext index creation supports one property at a time.

## Testing

Integration tests use Testcontainers (Neo4j by default; FalkorDB and Memgraph are activated via the `falkordb` and `memgraph` Spring profiles):

```bash
mvn test
```

## License

See LICENSE file in the root directory.
