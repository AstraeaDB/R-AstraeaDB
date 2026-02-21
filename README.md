# AstraeaDB

<!-- badges: start -->
<!-- badges: end -->

An R client for [AstraeaDB](https://github.com/astraeadb/astraeadb), a
cloud-native, AI-first graph database written in Rust. AstraeaDB provides
node and edge CRUD operations, graph traversals, temporal queries, vector
similarity search, hybrid graph-vector search, GQL query execution, and
GraphRAG (subgraph extraction with LLM integration).

## Installation

Install the development version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("astraeadb/R-AstraeaDB")
```

## Prerequisites

A running AstraeaDB server is required. See the
[AstraeaDB documentation](https://github.com/astraeadb/astraeadb) for
installation and startup instructions.

## Quick start

```r
library(AstraeaDB)

# Connect to a local AstraeaDB server
client <- astraea_connect()

# Create nodes
alice <- client$create_node("Person", list(name = "Alice", age = 30))
bob   <- client$create_node("Person", list(name = "Bob", age = 25))

# Create an edge
client$create_edge(alice, bob, "KNOWS", list(since = "2024-01-01"))

# Traverse the graph
client$neighbors(alice)
client$bfs(alice, max_depth = 3)
client$shortest_path(alice, bob)

# Run a GQL query
client$query("MATCH (p:Person) WHERE p.age > 20 RETURN p")

# Disconnect when done
client$disconnect()
```

## Features

### Core graph operations

- **Node and edge CRUD** -- create, read, update, and delete nodes and edges
- **Graph traversals** -- BFS, shortest path (weighted and unweighted),
  neighbor queries with direction and edge-type filtering
- **Batch operations** -- bulk create and delete for nodes and edges
- **Data frame integration** -- import nodes/edges from data frames and export
  query results back to data frames

### Vector and AI capabilities

- **Vector similarity search** -- k-nearest neighbor search over node embeddings
- **Hybrid search** -- blend graph proximity with vector similarity using a
  tunable alpha parameter
- **Semantic search** -- rank neighbors by concept similarity or perform greedy
  semantic walks
- **GraphRAG** -- extract subgraphs and feed them to an LLM for
  retrieval-augmented generation

### Temporal queries

- **Time-travel** -- query the graph as it existed at any point in time using
  edge validity windows (`valid_from` / `valid_to`)

### Anomaly detection

- **Deja vu algorithm** -- check anomaly status, retrieve statistics, and list
  active alerts

### Transport options

| Transport | Class | Use case |
|-----------|-------|----------|
| JSON/TCP (default) | `AstraeaClient` | General-purpose, always available |
| Apache Arrow Flight | `ArrowClient` | High-throughput columnar data transfer |
| Auto-select | `UnifiedClient` | Picks the best available transport |

Arrow Flight support is optional and activates automatically when the
[arrow](https://CRAN.R-project.org/package=arrow) package is installed.

## Clients

```r
# Standard JSON/TCP client
client <- astraea_connect(host = "127.0.0.1", port = 7687)

# Arrow Flight client (requires the arrow package)
arrow_client <- astraea_arrow_connect(uri = "grpc://localhost:7689")

# Unified client -- delegates CRUD to JSON/TCP, queries to Arrow when available
unified <- UnifiedClient$new(host = "127.0.0.1", port = 7687)
unified$connect()
```

## Vignettes

The package ships with four vignettes:
- **Introduction** -- overview of AstraeaDB and its data model
- **Getting started** -- detailed walkthrough of CRUD, traversals, and queries
- **Advanced features** -- vector search, temporal queries, and GraphRAG
- **PCAP analysis** -- worked example converting network packet captures into a
  graph

Build vignettes locally with:

```r
devtools::build_vignettes()
```

## Dependencies

| Package | Role |
|---------|------|
| jsonlite | JSON serialisation |
| R6 | Object-oriented client classes |
| arrow *(optional)* | Arrow Flight transport |

## License

MIT
