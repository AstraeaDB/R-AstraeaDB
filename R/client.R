#' @title AstraeaDB Client
#'
#' @description
#' R6 client for the AstraeaDB graph database using the JSON-over-TCP protocol.
#'
#' The client communicates with an AstraeaDB server by sending JSON-encoded
#' request lines over a TCP socket and reading JSON-encoded response lines back.
#' Each request contains a \code{"type"} field identifying the operation, and
#' each response contains a \code{"status"} field (\code{"ok"} or
#' \code{"error"}) along with a \code{"data"} payload on success.
#'
#' The client supports a comprehensive set of operations including:
#' \itemize{
#'   \item Node and edge CRUD (create, read, update, delete)
#'   \item Graph traversals (BFS, shortest path)
#'   \item Temporal queries (time-travel over edges with validity windows)
#'   \item GQL query execution
#'   \item Vector similarity search (k-NN)
#'   \item Hybrid graph-vector search
#'   \item Semantic neighbor ranking and semantic walks
#'   \item GraphRAG (subgraph extraction for LLM integration)
#'   \item Anomaly detection ("deja vu" algorithm)
#'   \item Batch and data frame import/export operations
#' }
#'
#' @section Connection:
#' Create a client with \code{AstraeaClient$new()}, then call
#' \code{$connect()} to open the TCP socket. Always call \code{$disconnect()}
#' when finished, or use \code{\link[base]{on.exit}} to ensure cleanup.
#'
#' @section Authentication:
#' If the server requires authentication, pass an \code{auth_token} to the
#' constructor. The token is automatically attached to every request.
#'
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom R6 R6Class
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to a local AstraeaDB server
#' client <- AstraeaClient$new()
#' client$connect()
#'
#' # Health check
#' client$ping()
#'
#' # Create nodes
#' alice_id <- client$create_node(
#'   labels = c("Person"),
#'   properties = list(name = "Alice", age = 30)
#' )
#' bob_id <- client$create_node(
#'   labels = c("Person"),
#'   properties = list(name = "Bob", age = 25)
#' )
#'
#' # Create an edge
#' edge_id <- client$create_edge(
#'   source = alice_id,
#'   target = bob_id,
#'   edge_type = "KNOWS",
#'   properties = list(since = 2020)
#' )
#'
#' # Traverse the graph
#' client$neighbors(alice_id, direction = "outgoing")
#' client$bfs(alice_id, max_depth = 2L)
#'
#' # Clean up
#' client$disconnect()
#' }
AstraeaClient <- R6::R6Class(
  "AstraeaClient",

  # ---------- public fields and methods ----------
  public = list(

    #' @field host Character scalar. Server hostname. Default \code{"127.0.0.1"}.
    host = NULL,

    #' @field port Integer scalar. Server port. Default \code{7687L}.
    port = NULL,

    #' @field con Socket connection object, or \code{NULL} when disconnected.
    con = NULL,

    #' @field auth_token Character scalar or \code{NULL}. Optional
    #'   authentication token sent with every request.
    auth_token = NULL,

    # ── Constructor ──────────────────────────────────────────

    #' @description
    #' Create a new AstraeaDB client.
    #'
    #' @param host Character scalar. Server hostname.
    #'   Default \code{"127.0.0.1"}.
    #' @param port Integer scalar. Server port. Default \code{7687L}.
    #' @param auth_token Character scalar or \code{NULL}. Optional
    #'   authentication token.
    #' @return An \code{AstraeaClient} object (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client <- AstraeaClient$new()
    #' client <- AstraeaClient$new(host = "db.example.com", port = 7688L)
    #' client <- AstraeaClient$new(auth_token = "my-secret-token")
    #' }
    initialize = function(host = "127.0.0.1", port = 7687L, auth_token = NULL) {
      stopifnot(
        is.character(host), length(host) == 1L, nchar(host) > 0L
      )
      port <- as.integer(port)
      stopifnot(
        is.integer(port), length(port) == 1L, !is.na(port),
        port > 0L, port <= 65535L
      )
      if (!is.null(auth_token)) {
        stopifnot(is.character(auth_token), length(auth_token) == 1L)
      }
      self$host       <- host
      self$port       <- port
      self$con        <- NULL
      self$auth_token <- auth_token
      invisible(self)
    },

    # ── Connection management ────────────────────────────────

    #' @description
    #' Open a TCP socket connection to the AstraeaDB server.
    #'
    #' @return The client object (invisibly), for method chaining.
    #'
    #' @examples
    #' \dontrun{
    #' client <- AstraeaClient$new()
    #' client$connect()
    #' }
    connect = function() {
      if (!is.null(self$con)) {
        message("Already connected. Disconnect first to reconnect.")
        return(invisible(self))
      }
      self$con <- socketConnection(
        host     = self$host,
        port     = self$port,
        open     = "r+b",
        blocking = TRUE,
        timeout  = 5
      )
      invisible(self)
    },

    #' @description
    #' Close the TCP socket connection.
    #'
    #' @return The client object (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client$disconnect()
    #' }
    disconnect = function() {
      if (!is.null(self$con)) {
        close(self$con)
        self$con <- NULL
      }
      invisible(self)
    },

    #' @description
    #' Check whether the client is currently connected.
    #'
    #' @return Logical scalar. \code{TRUE} if connected, \code{FALSE}
    #'   otherwise.
    #'
    #' @examples
    #' \dontrun{
    #' client$is_connected()
    #' }
    is_connected = function() {
      !is.null(self$con)
    },

    #' @description
    #' Print method showing connection status.
    #'
    #' @param ... Ignored. Present for compatibility with the generic.
    #' @return The client object (invisibly).
    print = function(...) {
      status <- if (self$is_connected()) "connected" else "disconnected"
      auth   <- if (!is.null(self$auth_token)) " (authenticated)" else ""
      cat(sprintf(
        "<AstraeaClient> %s:%d [%s%s]\n",
        self$host, self$port, status, auth
      ))
      invisible(self)
    },

    # ══════════════════════════════════════════════════════════
    # Health
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Health-check ping. Returns server information.
    #'
    #' @return A list with server information (e.g., \code{version},
    #'   \code{pong}).
    #'
    #' @examples
    #' \dontrun{
    #' info <- client$ping()
    #' message(info$version)
    #' }
    ping = function() {
      private$assert_connected()
      private$check(private$send(list(type = "Ping")))
    },

    # ══════════════════════════════════════════════════════════
    # Node CRUD
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Create a node.
    #'
    #' @param labels Character vector of labels for the node.
    #' @param properties Named list of node properties.
    #' @param embedding Optional numeric vector. An embedding associated
    #'   with the node for vector search.
    #' @return Integer scalar: the ID of the newly created node.
    #'
    #' @examples
    #' \dontrun{
    #' nid <- client$create_node(
    #'   labels = c("Person"),
    #'   properties = list(name = "Alice", age = 30),
    #'   embedding = c(0.1, 0.9, 0.3)
    #' )
    #' }
    create_node = function(labels, properties, embedding = NULL) {
      private$assert_connected()
      stopifnot(is.character(labels), length(labels) >= 1L)
      stopifnot(is.list(properties))
      if (!is.null(embedding)) {
        stopifnot(is.numeric(embedding))
      }
      req <- list(
        type       = "CreateNode",
        labels     = as.list(labels),
        properties = properties
      )
      if (!is.null(embedding)) req$embedding <- as.numeric(embedding)
      data <- private$check(private$send(req))
      as.integer(data$node_id)
    },

    #' @description
    #' Retrieve a node by its ID.
    #'
    #' @param node_id Integer scalar. The node ID to look up.
    #' @return A list with \code{labels} (character vector) and
    #'   \code{properties} (named list).
    #'
    #' @examples
    #' \dontrun{
    #' node <- client$get_node(1L)
    #' node$labels
    #' node$properties$name
    #' }
    get_node = function(node_id) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      stopifnot(!is.na(node_id))
      private$check(private$send(list(type = "GetNode", id = node_id)))
    },

    #' @description
    #' Update a node's properties using merge semantics. Existing
    #' properties not present in the update are preserved.
    #'
    #' @param node_id Integer scalar. The node ID to update.
    #' @param properties Named list of properties to merge.
    #' @return The server response data (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client$update_node(1L, list(city = "San Francisco"))
    #' }
    update_node = function(node_id, properties) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      stopifnot(!is.na(node_id))
      stopifnot(is.list(properties))
      invisible(private$check(private$send(list(
        type       = "UpdateNode",
        id         = node_id,
        properties = properties
      ))))
    },

    #' @description
    #' Delete a node and all edges connected to it.
    #'
    #' @param node_id Integer scalar. The node ID to delete.
    #' @return The server response data (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client$delete_node(1L)
    #' }
    delete_node = function(node_id) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      stopifnot(!is.na(node_id))
      invisible(private$check(private$send(list(
        type = "DeleteNode",
        id   = node_id
      ))))
    },

    # ══════════════════════════════════════════════════════════
    # Edge CRUD
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Create an edge between two nodes, with optional temporal validity.
    #'
    #' @param source Integer scalar. Source node ID.
    #' @param target Integer scalar. Target node ID.
    #' @param edge_type Character scalar. The relationship type
    #'   (e.g., \code{"KNOWS"}).
    #' @param properties Named list of edge properties.
    #'   Default \code{list()}.
    #' @param weight Numeric scalar. Edge weight. Default \code{1.0}.
    #' @param valid_from Numeric scalar or \code{NULL}. Start of temporal
    #'   validity window (milliseconds since epoch).
    #' @param valid_to Numeric scalar or \code{NULL}. End of temporal
    #'   validity window (milliseconds since epoch).
    #' @return Integer scalar: the ID of the newly created edge.
    #'
    #' @examples
    #' \dontrun{
    #' eid <- client$create_edge(
    #'   source    = 1L,
    #'   target    = 2L,
    #'   edge_type = "KNOWS",
    #'   weight    = 0.9
    #' )
    #' }
    create_edge = function(source, target, edge_type,
                           properties = list(), weight = 1.0,
                           valid_from = NULL, valid_to = NULL) {
      private$assert_connected()
      source <- as.integer(source)
      target <- as.integer(target)
      stopifnot(
        !is.na(source), !is.na(target),
        is.character(edge_type), length(edge_type) == 1L, nchar(edge_type) > 0L,
        is.list(properties),
        is.numeric(weight), length(weight) == 1L
      )
      req <- list(
        type       = "CreateEdge",
        source     = source,
        target     = target,
        edge_type  = edge_type,
        properties = properties,
        weight     = weight
      )
      if (!is.null(valid_from)) {
        stopifnot(is.numeric(valid_from), length(valid_from) == 1L)
        req$valid_from <- valid_from
      }
      if (!is.null(valid_to)) {
        stopifnot(is.numeric(valid_to), length(valid_to) == 1L)
        req$valid_to <- valid_to
      }
      data <- private$check(private$send(req))
      as.integer(data$edge_id)
    },

    #' @description
    #' Retrieve an edge by its ID.
    #'
    #' @param edge_id Integer scalar. The edge ID to look up.
    #' @return A list with \code{source}, \code{target}, \code{edge_type},
    #'   \code{properties}, and optional temporal fields.
    #'
    #' @examples
    #' \dontrun{
    #' edge <- client$get_edge(1L)
    #' edge$edge_type
    #' }
    get_edge = function(edge_id) {
      private$assert_connected()
      edge_id <- as.integer(edge_id)
      stopifnot(!is.na(edge_id))
      private$check(private$send(list(type = "GetEdge", id = edge_id)))
    },

    #' @description
    #' Update an edge's properties using merge semantics.
    #'
    #' @param edge_id Integer scalar. The edge ID to update.
    #' @param properties Named list of properties to merge.
    #' @return The server response data (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client$update_edge(1L, list(strength = "strong"))
    #' }
    update_edge = function(edge_id, properties) {
      private$assert_connected()
      edge_id <- as.integer(edge_id)
      stopifnot(!is.na(edge_id))
      stopifnot(is.list(properties))
      invisible(private$check(private$send(list(
        type       = "UpdateEdge",
        id         = edge_id,
        properties = properties
      ))))
    },

    #' @description
    #' Delete an edge.
    #'
    #' @param edge_id Integer scalar. The edge ID to delete.
    #' @return The server response data (invisibly).
    #'
    #' @examples
    #' \dontrun{
    #' client$delete_edge(1L)
    #' }
    delete_edge = function(edge_id) {
      private$assert_connected()
      edge_id <- as.integer(edge_id)
      stopifnot(!is.na(edge_id))
      invisible(private$check(private$send(list(
        type = "DeleteEdge",
        id   = edge_id
      ))))
    },

    # ══════════════════════════════════════════════════════════
    # Traversal
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Get neighbors of a node, optionally filtered by direction and edge
    #' type.
    #'
    #' @param node_id Integer scalar. The node whose neighbors to retrieve.
    #' @param direction Character scalar. One of \code{"outgoing"},
    #'   \code{"incoming"}, or \code{"both"}. Default \code{"outgoing"}.
    #' @param edge_type Character scalar or \code{NULL}. If non-\code{NULL},
    #'   only return neighbors connected by this edge type.
    #' @return A list of neighbor entries. Each entry is a list with at
    #'   least \code{node_id} and \code{edge_id}.
    #'
    #' @examples
    #' \dontrun{
    #' nbrs <- client$neighbors(1L, direction = "outgoing")
    #' nbrs_knows <- client$neighbors(1L, edge_type = "KNOWS")
    #' }
    neighbors = function(node_id, direction = "outgoing", edge_type = NULL) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      stopifnot(!is.na(node_id))
      direction <- match.arg(direction, c("outgoing", "incoming", "both"))
      req <- list(type = "Neighbors", id = node_id, direction = direction)
      if (!is.null(edge_type)) {
        stopifnot(is.character(edge_type), length(edge_type) == 1L)
        req$edge_type <- edge_type
      }
      data <- private$check(private$send(req))
      data$neighbors
    },

    #' @description
    #' Breadth-first search starting from a node.
    #'
    #' @param start Integer scalar. The starting node ID.
    #' @param max_depth Integer scalar. Maximum traversal depth.
    #'   Default \code{3L}.
    #' @return A list of entries, each a list with \code{node_id} (integer)
    #'   and \code{depth} (integer).
    #'
    #' @examples
    #' \dontrun{
    #' bfs_result <- client$bfs(1L, max_depth = 2L)
    #' }
    bfs = function(start, max_depth = 3L) {
      private$assert_connected()
      start     <- as.integer(start)
      max_depth <- as.integer(max_depth)
      stopifnot(!is.na(start), !is.na(max_depth), max_depth >= 0L)
      data <- private$check(private$send(list(
        type      = "Bfs",
        start     = start,
        max_depth = max_depth
      )))
      data$nodes
    },

    #' @description
    #' Find the shortest path between two nodes.
    #'
    #' @param from_node Integer scalar. Source node ID.
    #' @param to_node Integer scalar. Target node ID.
    #' @param weighted Logical scalar. If \code{TRUE}, use edge weights
    #'   (Dijkstra). If \code{FALSE}, use hop count. Default \code{FALSE}.
    #' @return A list with \code{path} (integer vector of node IDs),
    #'   \code{length} (hop count), and optionally \code{cost} (total weight
    #'   when \code{weighted = TRUE}).
    #'
    #' @examples
    #' \dontrun{
    #' sp <- client$shortest_path(1L, 5L, weighted = TRUE)
    #' sp$path
    #' sp$cost
    #' }
    shortest_path = function(from_node, to_node, weighted = FALSE) {
      private$assert_connected()
      from_node <- as.integer(from_node)
      to_node   <- as.integer(to_node)
      stopifnot(
        !is.na(from_node), !is.na(to_node),
        is.logical(weighted), length(weighted) == 1L
      )
      private$check(private$send(list(
        type     = "ShortestPath",
        from     = from_node,
        to       = to_node,
        weighted = weighted
      )))
    },

    # ══════════════════════════════════════════════════════════
    # Temporal Queries (Time-Travel)
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Get neighbors of a node at a specific point in time. Only edges
    #' whose temporal validity window includes \code{timestamp} are
    #' traversed.
    #'
    #' @param node_id Integer scalar. The node whose neighbors to retrieve.
    #' @param direction Character scalar. One of \code{"outgoing"},
    #'   \code{"incoming"}, or \code{"both"}. Default \code{"outgoing"}.
    #' @param timestamp Numeric scalar. Point in time as milliseconds since
    #'   the Unix epoch.
    #' @param edge_type Character scalar or \code{NULL}. Optional edge type
    #'   filter.
    #' @return A list of neighbor entries valid at the given timestamp.
    #'
    #' @examples
    #' \dontrun{
    #' # Neighbors as of January 1 2023 (ms since epoch)
    #' nbrs <- client$neighbors_at(1L, "outgoing", 1672531200000)
    #' }
    neighbors_at = function(node_id, direction = "outgoing",
                            timestamp, edge_type = NULL) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      stopifnot(!is.na(node_id))
      direction <- match.arg(direction, c("outgoing", "incoming", "both"))
      stopifnot(is.numeric(timestamp), length(timestamp) == 1L)
      req <- list(
        type      = "NeighborsAt",
        id        = node_id,
        direction = direction,
        timestamp = timestamp
      )
      if (!is.null(edge_type)) {
        stopifnot(is.character(edge_type), length(edge_type) == 1L)
        req$edge_type <- edge_type
      }
      data <- private$check(private$send(req))
      data$neighbors
    },

    #' @description
    #' Breadth-first search at a specific point in time.
    #'
    #' @param start Integer scalar. The starting node ID.
    #' @param max_depth Integer scalar. Maximum traversal depth.
    #'   Default \code{3L}.
    #' @param timestamp Numeric scalar. Point in time (ms since epoch).
    #' @return A list of entries with \code{node_id} and \code{depth}.
    #'
    #' @examples
    #' \dontrun{
    #' result <- client$bfs_at(1L, max_depth = 2L, timestamp = 1672531200000)
    #' }
    bfs_at = function(start, max_depth = 3L, timestamp) {
      private$assert_connected()
      start     <- as.integer(start)
      max_depth <- as.integer(max_depth)
      stopifnot(
        !is.na(start), !is.na(max_depth), max_depth >= 0L,
        is.numeric(timestamp), length(timestamp) == 1L
      )
      data <- private$check(private$send(list(
        type      = "BfsAt",
        start     = start,
        max_depth = max_depth,
        timestamp = timestamp
      )))
      data$nodes
    },

    #' @description
    #' Find the shortest path at a specific point in time.
    #'
    #' @param from_node Integer scalar. Source node ID.
    #' @param to_node Integer scalar. Target node ID.
    #' @param timestamp Numeric scalar. Point in time (ms since epoch).
    #' @param weighted Logical scalar. Use edge weights? Default
    #'   \code{FALSE}.
    #' @return A list with \code{path}, \code{length}, and optionally
    #'   \code{cost}.
    #'
    #' @examples
    #' \dontrun{
    #' sp <- client$shortest_path_at(1L, 5L, timestamp = 1672531200000)
    #' }
    shortest_path_at = function(from_node, to_node, timestamp,
                                weighted = FALSE) {
      private$assert_connected()
      from_node <- as.integer(from_node)
      to_node   <- as.integer(to_node)
      stopifnot(
        !is.na(from_node), !is.na(to_node),
        is.numeric(timestamp), length(timestamp) == 1L,
        is.logical(weighted), length(weighted) == 1L
      )
      private$check(private$send(list(
        type      = "ShortestPathAt",
        from      = from_node,
        to        = to_node,
        timestamp = timestamp,
        weighted  = weighted
      )))
    },

    # ══════════════════════════════════════════════════════════
    # GQL Query Execution
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Execute a GQL (Graph Query Language) query string.
    #'
    #' @param gql Character scalar. The GQL query to execute.
    #' @return The query result data as returned by the server.
    #'
    #' @examples
    #' \dontrun{
    #' result <- client$query("MATCH (p:Person) RETURN p.name, p.city")
    #' }
    query = function(gql) {
      private$assert_connected()
      stopifnot(is.character(gql), length(gql) == 1L, nchar(gql) > 0L)
      private$check(private$send(list(type = "Query", gql = gql)))
    },

    # ══════════════════════════════════════════════════════════
    # Vector Search
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Perform k-nearest neighbor vector similarity search.
    #'
    #' @param query_vector Numeric vector. The query embedding.
    #' @param k Integer scalar. Number of nearest neighbors to return.
    #'   Default \code{10L}.
    #' @return A list of result entries, each containing at least
    #'   \code{node_id} and \code{similarity}.
    #'
    #' @examples
    #' \dontrun{
    #' results <- client$vector_search(c(0.9, 0.1, 0.3), k = 5L)
    #' }
    vector_search = function(query_vector, k = 10L) {
      private$assert_connected()
      stopifnot(is.numeric(query_vector), length(query_vector) >= 1L)
      k <- as.integer(k)
      stopifnot(!is.na(k), k >= 1L)
      data <- private$check(private$send(list(
        type  = "VectorSearch",
        query = as.numeric(query_vector),
        k     = k
      )))
      data$results
    },

    # ══════════════════════════════════════════════════════════
    # Hybrid & Semantic Search
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Combined graph proximity and vector similarity search.
    #'
    #' The \code{alpha} parameter controls the blend between graph
    #' proximity and vector similarity. \code{alpha = 0.0} uses pure
    #' graph distance; \code{alpha = 1.0} uses pure vector similarity.
    #'
    #' @param anchor Integer scalar. Anchor node ID for graph proximity.
    #' @param query_vector Numeric vector. Query embedding.
    #' @param max_hops Integer scalar. Maximum graph hops from anchor.
    #'   Default \code{3L}.
    #' @param k Integer scalar. Number of results. Default \code{10L}.
    #' @param alpha Numeric scalar in \code{[0, 1]}. Blend factor.
    #'   Default \code{0.5}.
    #' @return A list of result entries with \code{node_id} and combined
    #'   scores.
    #'
    #' @examples
    #' \dontrun{
    #' results <- client$hybrid_search(
    #'   anchor = 1L,
    #'   query_vector = c(0.9, 0.1, 0.3),
    #'   k = 5L,
    #'   alpha = 0.7
    #' )
    #' }
    hybrid_search = function(anchor, query_vector, max_hops = 3L,
                             k = 10L, alpha = 0.5) {
      private$assert_connected()
      anchor   <- as.integer(anchor)
      max_hops <- as.integer(max_hops)
      k        <- as.integer(k)
      stopifnot(
        !is.na(anchor),
        is.numeric(query_vector), length(query_vector) >= 1L,
        !is.na(max_hops), max_hops >= 1L,
        !is.na(k), k >= 1L,
        is.numeric(alpha), length(alpha) == 1L,
        alpha >= 0, alpha <= 1
      )
      data <- private$check(private$send(list(
        type     = "HybridSearch",
        anchor   = anchor,
        query    = as.numeric(query_vector),
        max_hops = max_hops,
        k        = k,
        alpha    = alpha
      )))
      data$results
    },

    #' @description
    #' Get neighbors ranked by semantic similarity to a concept vector.
    #'
    #' @param node_id Integer scalar. The node whose neighbors to rank.
    #' @param concept Numeric vector. The concept embedding to rank
    #'   against.
    #' @param direction Character scalar. One of \code{"outgoing"},
    #'   \code{"incoming"}, or \code{"both"}. Default \code{"outgoing"}.
    #' @param k Integer scalar. Maximum number of ranked neighbors.
    #'   Default \code{10L}.
    #' @return A list of neighbor entries with \code{node_id} and
    #'   \code{similarity} scores.
    #'
    #' @examples
    #' \dontrun{
    #' nbrs <- client$semantic_neighbors(1L, c(0.0, 0.0, 1.0), k = 5L)
    #' }
    semantic_neighbors = function(node_id, concept, direction = "outgoing",
                                  k = 10L) {
      private$assert_connected()
      node_id <- as.integer(node_id)
      k       <- as.integer(k)
      stopifnot(
        !is.na(node_id),
        is.numeric(concept), length(concept) >= 1L,
        !is.na(k), k >= 1L
      )
      direction <- match.arg(direction, c("outgoing", "incoming", "both"))
      data <- private$check(private$send(list(
        type      = "SemanticNeighbors",
        id        = node_id,
        concept   = as.numeric(concept),
        direction = direction,
        k         = k
      )))
      data$neighbors
    },

    #' @description
    #' Greedy walk following edges whose targets are most similar to a
    #' concept vector.
    #'
    #' @param start Integer scalar. Starting node ID.
    #' @param concept Numeric vector. Concept embedding guiding the walk.
    #' @param max_hops Integer scalar. Maximum walk length.
    #'   Default \code{3L}.
    #' @return A list representing the walk path.
    #'
    #' @examples
    #' \dontrun{
    #' path <- client$semantic_walk(1L, c(0.1, 0.8, 0.5), max_hops = 4L)
    #' }
    semantic_walk = function(start, concept, max_hops = 3L) {
      private$assert_connected()
      start    <- as.integer(start)
      max_hops <- as.integer(max_hops)
      stopifnot(
        !is.na(start),
        is.numeric(concept), length(concept) >= 1L,
        !is.na(max_hops), max_hops >= 1L
      )
      data <- private$check(private$send(list(
        type     = "SemanticWalk",
        start    = start,
        concept  = as.numeric(concept),
        max_hops = max_hops
      )))
      data$path
    },

    # ══════════════════════════════════════════════════════════
    # GraphRAG (Subgraph Extraction + LLM)
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Extract a subgraph centered on a node and linearize it to text.
    #'
    #' @param center Integer scalar. Center node ID.
    #' @param hops Integer scalar. Radius in hops. Default \code{2L}.
    #' @param max_nodes Integer scalar. Maximum number of nodes to include.
    #'   Default \code{50L}.
    #' @param format Character scalar. Output format: one of
    #'   \code{"structured"}, \code{"prose"}, \code{"triples"}, or
    #'   \code{"json"}. Default \code{"structured"}.
    #' @return A list with extracted subgraph data including \code{text},
    #'   \code{node_count}, and \code{edge_count}.
    #'
    #' @examples
    #' \dontrun{
    #' sg <- client$extract_subgraph(1L, hops = 2L, max_nodes = 20L)
    #' sg$text
    #' }
    extract_subgraph = function(center, hops = 2L, max_nodes = 50L,
                                format = "structured") {
      private$assert_connected()
      center    <- as.integer(center)
      hops      <- as.integer(hops)
      max_nodes <- as.integer(max_nodes)
      stopifnot(
        !is.na(center),
        !is.na(hops), hops >= 1L,
        !is.na(max_nodes), max_nodes >= 1L
      )
      format <- match.arg(format, c("structured", "prose", "triples", "json"))
      private$check(private$send(list(
        type      = "ExtractSubgraph",
        center    = center,
        hops      = hops,
        max_nodes = max_nodes,
        format    = format
      )))
    },

    #' @description
    #' Execute a full GraphRAG pipeline: extract a subgraph and send it to
    #' a language model.
    #'
    #' Provide either \code{anchor} (a node ID to center the subgraph on)
    #' or \code{question_embedding} (a vector to locate the closest node
    #' via vector search), or both.
    #'
    #' @param question Character scalar. The natural-language question.
    #' @param anchor Integer scalar or \code{NULL}. Anchor node ID.
    #' @param question_embedding Numeric vector or \code{NULL}. Embedding
    #'   of the question for vector-based anchor selection.
    #' @param hops Integer scalar. Subgraph radius. Default \code{2L}.
    #' @param max_nodes Integer scalar. Maximum subgraph nodes.
    #'   Default \code{50L}.
    #' @param format Character scalar. Linearization format.
    #'   Default \code{"structured"}.
    #' @return A list with the RAG result, typically including an
    #'   \code{answer} field.
    #'
    #' @examples
    #' \dontrun{
    #' answer <- client$graph_rag(
    #'   question = "What does Alice work on?",
    #'   anchor = 1L
    #' )
    #' }
    graph_rag = function(question, anchor = NULL, question_embedding = NULL,
                         hops = 2L, max_nodes = 50L, format = "structured") {
      private$assert_connected()
      stopifnot(
        is.character(question), length(question) == 1L, nchar(question) > 0L
      )
      hops      <- as.integer(hops)
      max_nodes <- as.integer(max_nodes)
      stopifnot(!is.na(hops), hops >= 1L, !is.na(max_nodes), max_nodes >= 1L)
      format <- match.arg(format, c("structured", "prose", "triples", "json"))
      req <- list(
        type      = "GraphRag",
        question  = question,
        hops      = hops,
        max_nodes = max_nodes,
        format    = format
      )
      if (!is.null(anchor)) {
        anchor <- as.integer(anchor)
        stopifnot(!is.na(anchor))
        req$anchor <- anchor
      }
      if (!is.null(question_embedding)) {
        stopifnot(is.numeric(question_embedding))
        req$question_embedding <- as.numeric(question_embedding)
      }
      private$check(private$send(req))
    },

    # ══════════════════════════════════════════════════════════
    # Anomaly Detection
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Check the anomaly status of a node or edge.
    #'
    #' Uses AstraeaDB's "deja vu" anomaly detection algorithm to assess
    #' whether an entity's recent behavior is anomalous.
    #'
    #' @param entity_id Integer scalar. The node or edge ID to check.
    #' @param is_node Logical scalar. If \code{TRUE} (default), check a
    #'   node; if \code{FALSE}, check an edge.
    #' @return A list with anomaly score information, or a message
    #'   indicating no data is available yet.
    #'
    #' @examples
    #' \dontrun{
    #' result <- client$anomaly_check(1L)
    #' result <- client$anomaly_check(5L, is_node = FALSE)
    #' }
    anomaly_check = function(entity_id, is_node = TRUE) {
      private$assert_connected()
      entity_id <- as.integer(entity_id)
      stopifnot(!is.na(entity_id))
      stopifnot(is.logical(is_node), length(is_node) == 1L)
      private$check(private$send(list(
        type    = "AnomalyCheck",
        id      = entity_id,
        is_node = is_node
      )))
    },

    #' @description
    #' Get detailed anomaly statistics for a node or edge.
    #'
    #' Returns more detailed information than \code{anomaly_check()},
    #' including historical anomaly scores and thresholds.
    #'
    #' @param entity_id Integer scalar. The node or edge ID.
    #' @param is_node Logical scalar. If \code{TRUE} (default), query a
    #'   node; if \code{FALSE}, query an edge.
    #' @return A list with detailed anomaly statistics.
    #'
    #' @examples
    #' \dontrun{
    #' stats <- client$anomaly_stats(1L)
    #' stats <- client$anomaly_stats(5L, is_node = FALSE)
    #' }
    anomaly_stats = function(entity_id, is_node = TRUE) {
      private$assert_connected()
      entity_id <- as.integer(entity_id)
      stopifnot(!is.na(entity_id))
      stopifnot(is.logical(is_node), length(is_node) == 1L)
      private$check(private$send(list(
        type    = "AnomalyStats",
        id      = entity_id,
        is_node = is_node
      )))
    },

    #' @description
    #' Get all active anomaly alerts.
    #'
    #' Returns a list of currently active anomaly alerts detected by the
    #' server's "deja vu" algorithm.
    #'
    #' @return A list of alert entries. Each entry describes an anomalous
    #'   entity and its anomaly score.
    #'
    #' @examples
    #' \dontrun{
    #' alerts <- client$anomaly_alerts()
    #' for (a in alerts) {
    #'   cat(sprintf("Entity %d: score %.2f\n", a$id, a$score))
    #' }
    #' }
    anomaly_alerts = function() {
      private$assert_connected()
      data <- private$check(private$send(list(type = "AnomalyAlerts")))
      data$alerts
    },

    # ══════════════════════════════════════════════════════════
    # Batch Operations
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Create multiple nodes in a single batch.
    #'
    #' @param nodes_list A list of node specifications. Each element must
    #'   be a list with \code{labels} (character vector) and
    #'   \code{properties} (named list). An optional \code{embedding}
    #'   (numeric vector) may be included.
    #' @return An integer vector of created node IDs, in the same order as
    #'   the input.
    #'
    #' @examples
    #' \dontrun{
    #' ids <- client$create_nodes(list(
    #'   list(labels = "Person", properties = list(name = "Alice")),
    #'   list(labels = "Person", properties = list(name = "Bob"))
    #' ))
    #' }
    create_nodes = function(nodes_list) {
      private$assert_connected()
      stopifnot(is.list(nodes_list), length(nodes_list) >= 1L)
      vapply(nodes_list, function(node) {
        self$create_node(
          labels     = node$labels,
          properties = node$properties,
          embedding  = node$embedding
        )
      }, integer(1))
    },

    #' @description
    #' Create multiple edges in a single batch.
    #'
    #' @param edges_list A list of edge specifications. Each element must
    #'   be a list with \code{source}, \code{target}, and \code{edge_type}.
    #'   Optional fields: \code{properties}, \code{weight},
    #'   \code{valid_from}, \code{valid_to}.
    #' @return An integer vector of created edge IDs, in the same order as
    #'   the input.
    #'
    #' @examples
    #' \dontrun{
    #' eids <- client$create_edges(list(
    #'   list(source = 1L, target = 2L, edge_type = "KNOWS"),
    #'   list(source = 2L, target = 3L, edge_type = "FOLLOWS", weight = 0.5)
    #' ))
    #' }
    create_edges = function(edges_list) {
      private$assert_connected()
      stopifnot(is.list(edges_list), length(edges_list) >= 1L)
      vapply(edges_list, function(edge) {
        self$create_edge(
          source     = edge$source,
          target     = edge$target,
          edge_type  = edge$edge_type,
          properties = if (!is.null(edge$properties)) edge$properties else list(),
          weight     = if (!is.null(edge$weight)) edge$weight else 1.0,
          valid_from = edge$valid_from,
          valid_to   = edge$valid_to
        )
      }, integer(1))
    },

    #' @description
    #' Delete multiple nodes. Errors for individual nodes are silently
    #' skipped.
    #'
    #' @param node_ids Integer vector of node IDs to delete.
    #' @return Integer scalar: the count of successfully deleted nodes.
    #'
    #' @examples
    #' \dontrun{
    #' deleted <- client$delete_nodes(c(1L, 2L, 3L))
    #' }
    delete_nodes = function(node_ids) {
      private$assert_connected()
      stopifnot(is.numeric(node_ids), length(node_ids) >= 1L)
      node_ids <- as.integer(node_ids)
      count <- 0L
      for (nid in node_ids) {
        tryCatch({
          self$delete_node(nid)
          count <- count + 1L
        }, error = function(e) NULL)
      }
      count
    },

    #' @description
    #' Delete multiple edges. Errors for individual edges are silently
    #' skipped.
    #'
    #' @param edge_ids Integer vector of edge IDs to delete.
    #' @return Integer scalar: the count of successfully deleted edges.
    #'
    #' @examples
    #' \dontrun{
    #' deleted <- client$delete_edges(c(1L, 2L))
    #' }
    delete_edges = function(edge_ids) {
      private$assert_connected()
      stopifnot(is.numeric(edge_ids), length(edge_ids) >= 1L)
      edge_ids <- as.integer(edge_ids)
      count <- 0L
      for (eid in edge_ids) {
        tryCatch({
          self$delete_edge(eid)
          count <- count + 1L
        }, error = function(e) NULL)
      }
      count
    },

    # ══════════════════════════════════════════════════════════
    # Data Frame Import / Export
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Import nodes from a \code{data.frame}.
    #'
    #' Each row becomes a node. One column supplies the label(s), and the
    #' remaining columns (excluding any embedding columns) become node
    #' properties.
    #'
    #' @param df A \code{data.frame} with one row per node.
    #' @param label_col Character scalar. Name of the column containing
    #'   node labels. Default \code{"label"}.
    #' @param id_col Character scalar or \code{NULL}. If non-\code{NULL},
    #'   this column is stored in the node properties as an external
    #'   identifier.
    #' @param embedding_cols Character vector or \code{NULL}. Column names
    #'   whose values form the embedding vector.
    #' @return An integer vector of created node IDs.
    #'
    #' @examples
    #' \dontrun{
    #' df <- data.frame(
    #'   label = c("Person", "Person"),
    #'   name  = c("Alice", "Bob"),
    #'   age   = c(30, 25),
    #'   stringsAsFactors = FALSE
    #' )
    #' ids <- client$import_nodes_df(df)
    #' }
    import_nodes_df = function(df, label_col = "label", id_col = NULL,
                               embedding_cols = NULL) {
      private$assert_connected()
      stopifnot(
        is.data.frame(df), nrow(df) >= 1L,
        is.character(label_col), length(label_col) == 1L,
        label_col %in% names(df)
      )
      if (!is.null(id_col)) {
        stopifnot(is.character(id_col), length(id_col) == 1L,
                  id_col %in% names(df))
      }
      if (!is.null(embedding_cols)) {
        stopifnot(is.character(embedding_cols),
                  all(embedding_cols %in% names(df)))
      }

      ids <- integer(nrow(df))
      for (i in seq_len(nrow(df))) {
        row <- df[i, , drop = FALSE]

        # Extract labels as a character vector
        labels <- as.character(row[[label_col]])

        # Extract embedding if specified
        embedding <- NULL
        if (!is.null(embedding_cols)) {
          embedding <- as.numeric(row[, embedding_cols, drop = TRUE])
        }

        # Build properties from remaining columns
        prop_cols  <- setdiff(names(df), c(label_col, embedding_cols))
        properties <- as.list(row[, prop_cols, drop = FALSE])

        ids[i] <- self$create_node(labels, properties, embedding)
      }
      ids
    },

    #' @description
    #' Import edges from a \code{data.frame}.
    #'
    #' Each row becomes an edge. Columns supply the source/target node IDs,
    #' edge type, and optionally weight and temporal bounds. Remaining
    #' columns become edge properties.
    #'
    #' @param df A \code{data.frame} with one row per edge.
    #' @param source_col Character scalar. Column with source node IDs.
    #'   Default \code{"source"}.
    #' @param target_col Character scalar. Column with target node IDs.
    #'   Default \code{"target"}.
    #' @param type_col Character scalar. Column with edge type strings.
    #'   Default \code{"type"}.
    #' @param weight_col Character scalar or \code{NULL}. Column with edge
    #'   weights.
    #' @param valid_from_col Character scalar or \code{NULL}. Column with
    #'   temporal start (ms since epoch).
    #' @param valid_to_col Character scalar or \code{NULL}. Column with
    #'   temporal end (ms since epoch).
    #' @return An integer vector of created edge IDs.
    #'
    #' @examples
    #' \dontrun{
    #' edf <- data.frame(
    #'   source = c(1L, 2L),
    #'   target = c(2L, 3L),
    #'   type   = c("KNOWS", "FOLLOWS"),
    #'   stringsAsFactors = FALSE
    #' )
    #' eids <- client$import_edges_df(edf)
    #' }
    import_edges_df = function(df, source_col = "source",
                               target_col = "target", type_col = "type",
                               weight_col = NULL, valid_from_col = NULL,
                               valid_to_col = NULL) {
      private$assert_connected()
      stopifnot(
        is.data.frame(df), nrow(df) >= 1L,
        is.character(source_col), length(source_col) == 1L,
        source_col %in% names(df),
        is.character(target_col), length(target_col) == 1L,
        target_col %in% names(df),
        is.character(type_col), length(type_col) == 1L,
        type_col %in% names(df)
      )
      if (!is.null(weight_col)) {
        stopifnot(is.character(weight_col), weight_col %in% names(df))
      }
      if (!is.null(valid_from_col)) {
        stopifnot(is.character(valid_from_col),
                  valid_from_col %in% names(df))
      }
      if (!is.null(valid_to_col)) {
        stopifnot(is.character(valid_to_col),
                  valid_to_col %in% names(df))
      }

      ids <- integer(nrow(df))
      for (i in seq_len(nrow(df))) {
        row <- df[i, , drop = FALSE]

        source_id  <- row[[source_col]]
        target_id  <- row[[target_col]]
        edge_type  <- row[[type_col]]

        weight     <- if (!is.null(weight_col)) row[[weight_col]] else 1.0
        valid_from <- if (!is.null(valid_from_col)) row[[valid_from_col]] else NULL
        valid_to   <- if (!is.null(valid_to_col)) row[[valid_to_col]] else NULL

        # Build properties from remaining columns
        exclude   <- c(source_col, target_col, type_col,
                       weight_col, valid_from_col, valid_to_col)
        prop_cols <- setdiff(names(df), exclude)
        properties <- as.list(row[, prop_cols, drop = FALSE])

        ids[i] <- self$create_edge(source_id, target_id, edge_type,
                                   properties, weight,
                                   valid_from, valid_to)
      }
      ids
    },

    #' @description
    #' Export nodes to a \code{data.frame} with \code{node_id}, a
    #' comma-separated \code{labels} column, and flattened property
    #' columns.
    #'
    #' @param node_ids Integer vector of node IDs to export.
    #' @return A \code{data.frame} with one row per node.
    #'
    #' @examples
    #' \dontrun{
    #' df <- client$export_nodes_df(c(1L, 2L, 3L))
    #' }
    export_nodes_df = function(node_ids) {
      private$assert_connected()
      node_ids <- as.integer(node_ids)
      if (length(node_ids) == 0L) return(data.frame())

      rows <- lapply(node_ids, function(nid) {
        node  <- self$get_node(nid)
        props <- if (length(node$properties) > 0L) {
          as.data.frame(node$properties, stringsAsFactors = FALSE)
        } else {
          data.frame()
        }
        cbind(
          data.frame(
            node_id = nid,
            labels  = paste(unlist(node$labels), collapse = ","),
            stringsAsFactors = FALSE
          ),
          props
        )
      })

      # Align columns across rows (different nodes may have different
      # properties)
      all_cols <- unique(unlist(lapply(rows, names)))
      rows <- lapply(rows, function(r) {
        missing <- setdiff(all_cols, names(r))
        for (col in missing) r[[col]] <- NA
        r[, all_cols, drop = FALSE]
      })
      do.call(rbind, rows)
    },

    #' @description
    #' Run BFS from a starting node and return the results as a
    #' \code{data.frame} that includes node details.
    #'
    #' @param start Integer scalar. Starting node ID.
    #' @param max_depth Integer scalar. Maximum BFS depth.
    #'   Default \code{3L}.
    #' @return A \code{data.frame} with \code{node_id}, \code{depth},
    #'   \code{labels}, and flattened property columns.
    #'
    #' @examples
    #' \dontrun{
    #' bfs_df <- client$export_bfs_df(1L, max_depth = 2L)
    #' }
    export_bfs_df = function(start, max_depth = 3L) {
      private$assert_connected()
      bfs_result <- self$bfs(start, max_depth)
      if (length(bfs_result) == 0L) return(data.frame())

      rows <- lapply(bfs_result, function(entry) {
        node  <- self$get_node(entry$node_id)
        props <- if (length(node$properties) > 0L) {
          as.data.frame(node$properties, stringsAsFactors = FALSE)
        } else {
          data.frame()
        }
        cbind(
          data.frame(
            node_id = entry$node_id,
            depth   = entry$depth,
            labels  = paste(unlist(node$labels), collapse = ","),
            stringsAsFactors = FALSE
          ),
          props
        )
      })

      all_cols <- unique(unlist(lapply(rows, names)))
      rows <- lapply(rows, function(r) {
        missing <- setdiff(all_cols, names(r))
        for (col in missing) r[[col]] <- NA
        r[, all_cols, drop = FALSE]
      })
      do.call(rbind, rows)
    },

    # ══════════════════════════════════════════════════════════
    # Utility Functions
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Convert a list of search results to a \code{data.frame}.
    #'
    #' This is a convenience method for converting results from
    #' \code{vector_search()}, \code{hybrid_search()}, or similar methods
    #' into a tabular format.
    #'
    #' @param results A list of result entries (e.g., from
    #'   \code{vector_search()}). Each entry should be a list with named
    #'   elements.
    #' @return A \code{data.frame} with one row per result entry. Returns
    #'   an empty \code{data.frame} if \code{results} is empty.
    #'
    #' @examples
    #' \dontrun{
    #' results <- client$vector_search(c(0.9, 0.1, 0.3), k = 5L)
    #' df <- client$results_to_dataframe(results)
    #' }
    results_to_dataframe = function(results) {
      if (length(results) == 0L) return(data.frame())
      do.call(rbind, lapply(results, as.data.frame))
    },

    #' @description
    #' Fetch multiple nodes by ID and return as a \code{data.frame}.
    #'
    #' Similar to \code{export_nodes_df()} but preserves the \code{labels}
    #' column as a list column (using \code{\link[base]{I}}) rather than
    #' collapsing to a comma-separated string.
    #'
    #' @param node_ids Integer vector of node IDs to fetch.
    #' @return A \code{data.frame} with columns \code{id}, \code{labels}
    #'   (list column), and flattened property columns.
    #'
    #' @examples
    #' \dontrun{
    #' df <- client$nodes_to_dataframe(c(1L, 2L, 3L))
    #' df$labels[[1]]
    #' }
    nodes_to_dataframe = function(node_ids) {
      private$assert_connected()
      node_ids <- as.integer(node_ids)
      if (length(node_ids) == 0L) return(data.frame())
      rows <- lapply(node_ids, function(nid) {
        node <- self$get_node(nid)
        data.frame(
          id     = nid,
          labels = I(list(node$labels)),
          as.data.frame(node$properties, stringsAsFactors = FALSE),
          stringsAsFactors = FALSE
        )
      })
      do.call(rbind, rows)
    }
  ),

  # ---------- private methods ----------
  private = list(

    # Send a JSON request and parse the response.
    send = function(request) {
      if (!is.null(self$auth_token)) {
        request$auth_token <- self$auth_token
      }
      line <- paste0(
        jsonlite::toJSON(request, auto_unbox = TRUE, na = "null", null = "null"),
        "\n"
      )
      writeLines(line, self$con, sep = "")
      flush(self$con)
      response_line <- readLines(self$con, n = 1L, warn = FALSE)
      if (length(response_line) == 0L) {
        stop("Server closed the connection unexpectedly.", call. = FALSE)
      }
      jsonlite::fromJSON(response_line, simplifyVector = FALSE)
    },

    # Check a server response for errors.
    check = function(response) {
      if (identical(response$status, "error")) {
        msg <- response$message %||% "Unknown error"
        if (!is.null(response$code)) {
          msg <- sprintf("[%s] %s", response$code, msg)
        }
        if (!is.null(response$details)) {
          msg <- sprintf("%s\nDetails: %s", msg, response$details)
        }
        stop(paste("AstraeaDB error:", msg), call. = FALSE)
      }
      response$data
    },

    # Assert that the client is currently connected.
    assert_connected = function() {
      if (is.null(self$con)) {
        stop("Not connected. Call $connect() first.", call. = FALSE)
      }
    }
  )
)

