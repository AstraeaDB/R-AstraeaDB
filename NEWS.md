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
