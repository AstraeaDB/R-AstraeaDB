#' @description
#' The AstraeaDB package provides an R client for the AstraeaDB graph database,
#' a cloud-native, AI-first graph database written in Rust. AstraeaDB combines
#' a Vector-Property Graph model with an HNSW vector index, enabling both
#' structural graph traversals and semantic similarity search.
#'
#' @section Client Classes:
#' \itemize{
#'   \item \code{\link{AstraeaClient}}: JSON/TCP client (always available). The
#'     primary client for all graph operations.
#'   \item \code{\link{ArrowClient}}: Apache Arrow Flight client for
#'     high-performance bulk queries. Requires the \pkg{arrow} package.
#'   \item \code{\link{UnifiedClient}}: Auto-selects the best available
#'     transport.
#' }
#'
#' @section Main Features:
#' \itemize{
#'   \item \strong{Node and Edge CRUD}: Create, read, update, and delete nodes
#'     and edges with arbitrary properties.
#'   \item \strong{Graph Traversals}: BFS, shortest path (weighted/unweighted).
#'   \item \strong{Temporal Queries}: Query the graph at specific points in time
#'     using edge validity intervals.
#'   \item \strong{Vector Search}: k-nearest-neighbor search using HNSW index.
#'   \item \strong{Hybrid Search}: Combine graph proximity with vector
#'     similarity.
#'   \item \strong{GQL Queries}: Execute GQL/Cypher query strings.
#'   \item \strong{GraphRAG}: Extract subgraphs and generate LLM context.
#'   \item \strong{Anomaly Detection}: Monitor entity behavior and detect
#'     anomalies using the "deja vu" algorithm.
#'   \item \strong{Data Frame Integration}: Import/export between data frames
#'     and the graph.
#' }
#'
#' @section Getting Started:
#' \preformatted{
#' # Start the AstraeaDB server first, then:
#' client <- astraea_connect()
#' node_id <- client$create_node("Person", list(name = "Alice", age = 30))
#' client$get_node(node_id)
#' client$disconnect()
#' }
#'
#' @keywords internal
"_PACKAGE"
