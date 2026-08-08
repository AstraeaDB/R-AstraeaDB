# AstraeaDB 0.2.1

## CRAN resubmission

* The `Description` field no longer opens with a redundant "an R", and single
  quotes are now limited to software and package names ('AstraeaDB', 'Apache
  Arrow Flight', 'arrow') rather than acronyms such as BFS or GQL. Acronyms
  that remain are spelled out on first use.
* No `\dontrun{}` remains in the package. Two examples that need no server run
  unconditionally; the rest are `\donttest{}` guarded by
  `astraea_server_available()`, so they are a no-op without a server and
  genuinely execute with one.
* Examples touching the optional Arrow Flight transport are wrapped in
  `requireNamespace("arrow", quietly = TRUE)`, and 'arrow' stays in Suggests.

## Bug fixes

* `UnifiedClient$query_df()` no longer fails with "arguments imply differing
  number of rows" when a matched node lacks one of the requested properties, or
  when rows carry different property sets. An absent property now becomes `NA`
  and the result takes the union of columns across rows. This affected ordinary
  queries: `MATCH (n:Person) RETURN n.name, n.age` errored whenever any person
  had no age.

## Documentation

* Examples no longer reference hard-coded node and edge IDs such as `1L`. Each
  creates the nodes it needs and uses the returned IDs, so it is self-contained.
* Examples that store embeddings size them from `client$ping()$vector_dim`
  instead of a fixed three-element vector, since a store pins its embedding
  width on first insert.

# AstraeaDB 0.2.0

## New features

* Graph algorithms are now exposed as client methods, computed server-side:
  `run_pagerank()`, `run_louvain()`, `run_connected_components()`,
  `run_degree_centrality()`, and `run_betweenness_centrality()`. Each accepts an
  optional `nodes` argument to restrict the computation to a node subset.
* Depth-first traversal: `dfs()` and the time-travel variant `dfs_at()`.
* Lookups: `find_by_label()`, `find_edge_by_type()`, and bulk `delete_by_label()`.
* Raw subgraph export via `get_subgraph()` and graph-wide `graph_stats()`.
* All of the above are available on both `AstraeaClient` and `UnifiedClient`.

## Breaking changes

* Removed the anomaly-detection methods (`anomaly_check()`, `anomaly_stats()`,
  and `anomaly_alerts()`); the AstraeaDB server no longer provides this
  operation.

## Documentation

* `vector_search()` results are now documented as returning `distance` (smaller
  is closer), with a legacy `score` alias retained for backward compatibility.
  `semantic_neighbors()` results likewise carry `distance`, while
  `hybrid_search()` returns a combined `score`.
