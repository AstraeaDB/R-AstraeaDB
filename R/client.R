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
#'   \item Node and edge lookup by label or edge type
#'   \item Graph traversals (BFS, DFS, shortest path)
#'   \item Temporal queries (time-travel over edges with validity windows)
#'   \item Graph algorithms (PageRank, Louvain community detection,
#'     connected components, degree and betweenness centrality)
#'   \item GQL query execution
#'   \item Vector similarity search (k-NN)
#'   \item Hybrid graph-vector search
#'   \item Semantic neighbor ranking and semantic walks
#'   \item GraphRAG (subgraph extraction for LLM integration)
#'   \item Graph statistics and raw subgraph export
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
#' \donttest{
#' if (astraea_server_available()) {
#'   # Connect to a local AstraeaDB server
#'   client <- AstraeaClient$new()
#'   client$connect()
#'
#'   # Health check
#'   client$ping()
#'
#'   # Create nodes
#'   alice_id <- client$create_node(
#'     labels = c("Person"),
#'     properties = list(name = "Alice", age = 30)
#'   )
#'   bob_id <- client$create_node(
#'     labels = c("Person"),
#'     properties = list(name = "Bob", age = 25)
#'   )
#'
#'   # Create an edge
#'   edge_id <- client$create_edge(
#'     source = alice_id,
#'     target = bob_id,
#'     edge_type = "KNOWS",
#'     properties = list(since = 2020)
#'   )
#'
#'   # Traverse the graph
#'   client$neighbors(alice_id, direction = "outgoing")
#'   client$bfs(alice_id, max_depth = 2L)
#'
#'   # Clean up
#'   client$disconnect()
#' }
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
    #' client <- AstraeaClient$new()
    #' client <- AstraeaClient$new(host = "db.example.com", port = 7688L)
    #' client <- AstraeaClient$new(auth_token = "my-secret-token")
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- AstraeaClient$new()
    #'   client$connect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   client$disconnect()
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   client$is_connected()
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   info <- client$ping()
    #'   message(info$version)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   dim <- client$ping()$vector_dim
    #'   nid <- client$create_node(
    #'     labels = c("Person"),
    #'     properties = list(name = "Alice", age = 30),
    #'     embedding = rep(0.1, dim)
    #'   )
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   node <- client$get_node(a)
    #'   node$labels
    #'   node$properties$name
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   client$update_node(a, list(city = "San Francisco"))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   client$delete_node(a)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   eid <- client$create_edge(
    #'     source    = a,
    #'     target    = b,
    #'     edge_type = "KNOWS",
    #'     weight    = 0.9
    #'   )
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   e <- client$create_edge(a, b, "KNOWS")
    #'   edge <- client$get_edge(e)
    #'   edge$edge_type
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   e <- client$create_edge(a, b, "KNOWS")
    #'   client$update_edge(e, list(strength = "strong"))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   e <- client$create_edge(a, b, "KNOWS")
    #'   client$delete_edge(e)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   nbrs <- client$neighbors(a, direction = "outgoing")
    #'   nbrs_knows <- client$neighbors(a, edge_type = "KNOWS")
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   bfs_result <- client$bfs(a, max_depth = b)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   sp <- client$shortest_path(a, b, weighted = TRUE)
    #'   sp$path
    #'   sp$cost
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   # Neighbors as of January 1 2023 (ms since epoch)
    #'   nbrs <- client$neighbors_at(a, "outgoing", 1672531200000)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   result <- client$bfs_at(a, max_depth = b, timestamp = 1672531200000)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   sp <- client$shortest_path_at(a, b, timestamp = 1672531200000)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   result <- client$query("MATCH (p:Person) RETURN p.name, p.city")
    #'   client$disconnect()
    #' }
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
    #'   \code{node_id} and \code{distance} (smaller is closer). A legacy
    #'   \code{score} alias equal to \code{distance} is also present for
    #'   backward compatibility with older clients.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   dim <- client$ping()$vector_dim
    #'   results <- client$vector_search(rep(0.1, dim), k = 5L)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   dim <- client$ping()$vector_dim
    #'   results <- client$hybrid_search(
    #'     anchor = a,
    #'     query_vector = rep(0.1, dim),
    #'     k = b,
    #'     alpha = 0.7
    #'   )
    #'   client$disconnect()
    #' }
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
    #'   \code{distance} (smaller is closer to the concept).
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   dim <- client$ping()$vector_dim
    #'   nbrs <- client$semantic_neighbors(a, rep(0.1, dim), k = b)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   dim <- client$ping()$vector_dim
    #'   path <- client$semantic_walk(a, rep(0.1, dim), max_hops = 4L)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   sg <- client$extract_subgraph(a, hops = b, max_nodes = 20L)
    #'   sg$text
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   answer <- client$graph_rag(
    #'     question = "What does Alice work on?",
    #'     anchor = a
    #'   )
    #'   client$disconnect()
    #' }
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
    # Depth-First Search
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Depth-first search starting from a node.
    #'
    #' @param start Integer scalar. The starting node ID.
    #' @param max_depth Integer scalar. Maximum traversal depth.
    #'   Default \code{3L}.
    #' @return A list of node IDs (integers) in depth-first visitation order.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   visited <- client$dfs(a, max_depth = b)
    #'   client$disconnect()
    #' }
    #' }
    dfs = function(start, max_depth = 3L) {
      private$assert_connected()
      start     <- as.integer(start)
      max_depth <- as.integer(max_depth)
      stopifnot(!is.na(start), !is.na(max_depth), max_depth >= 0L)
      data <- private$check(private$send(list(
        type      = "Dfs",
        start     = start,
        max_depth = max_depth
      )))
      data$nodes
    },

    #' @description
    #' Depth-first search as of a specific point in time.
    #'
    #' @param start Integer scalar. The starting node ID.
    #' @param max_depth Integer scalar. Maximum traversal depth.
    #'   Default \code{3L}.
    #' @param timestamp Numeric scalar. Point in time (ms since epoch).
    #' @return A list of node IDs (integers) in depth-first visitation
    #'   order as of \code{timestamp}.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   visited <- client$dfs_at(a, max_depth = b, timestamp = 1672531200000)
    #'   client$disconnect()
    #' }
    #' }
    dfs_at = function(start, max_depth = 3L, timestamp) {
      private$assert_connected()
      start     <- as.integer(start)
      max_depth <- as.integer(max_depth)
      stopifnot(
        !is.na(start), !is.na(max_depth), max_depth >= 0L,
        is.numeric(timestamp), length(timestamp) == 1L
      )
      data <- private$check(private$send(list(
        type      = "DfsAt",
        start     = start,
        max_depth = max_depth,
        timestamp = timestamp
      )))
      data$nodes
    },

    # ══════════════════════════════════════════════════════════
    # Label and Edge-Type Lookups
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Find all nodes carrying a given label.
    #'
    #' @param label Character scalar. The node label to match.
    #' @return A list of matching node IDs (integers).
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   ids <- client$find_by_label("Person")
    #'   client$disconnect()
    #' }
    #' }
    find_by_label = function(label) {
      private$assert_connected()
      stopifnot(is.character(label), length(label) == 1L)
      data <- private$check(private$send(list(
        type  = "FindByLabel",
        label = label
      )))
      data$node_ids
    },

    #' @description
    #' Find all edges of a given edge type.
    #'
    #' @param edge_type Character scalar. The edge type to match.
    #' @return A list of entries, each a list with \code{edge_id},
    #'   \code{source}, and \code{target} node IDs.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   edges <- client$find_edge_by_type("KNOWS")
    #'   client$disconnect()
    #' }
    #' }
    find_edge_by_type = function(edge_type) {
      private$assert_connected()
      stopifnot(is.character(edge_type), length(edge_type) == 1L)
      data <- private$check(private$send(list(
        type      = "FindEdgeByType",
        edge_type = edge_type
      )))
      data$edges
    },

    #' @description
    #' Delete every node carrying a given label, along with all its edges.
    #'
    #' @param label Character scalar. The node label to match.
    #' @return Integer scalar: the number of nodes deleted.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   n_removed <- client$delete_by_label("Temporary")
    #'   client$disconnect()
    #' }
    #' }
    delete_by_label = function(label) {
      private$assert_connected()
      stopifnot(is.character(label), length(label) == 1L)
      data <- private$check(private$send(list(
        type  = "DeleteByLabel",
        label = label
      )))
      as.integer(data$deleted)
    },

    # ══════════════════════════════════════════════════════════
    # Subgraph and Statistics
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Retrieve the raw subgraph (nodes and edges) around a center node,
    #' suitable for visualization or client-side processing.
    #'
    #' @param center Integer scalar. The center node ID.
    #' @param hops Integer scalar. Neighborhood radius in hops.
    #'   Default \code{3L}.
    #' @param max_nodes Integer scalar. Maximum number of nodes to return.
    #'   Default \code{50L}.
    #' @return A list with \code{nodes} (each a list with \code{id},
    #'   \code{labels}, \code{properties}, \code{has_embedding}) and
    #'   \code{edges} (each a list with \code{id}, \code{source},
    #'   \code{target}, \code{edge_type}, \code{properties}, \code{weight},
    #'   \code{valid_from}, \code{valid_to}).
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   sg <- client$get_subgraph(a, hops = b, max_nodes = 100L)
    #'   client$disconnect()
    #' }
    #' }
    get_subgraph = function(center, hops = 3L, max_nodes = 50L) {
      private$assert_connected()
      center    <- as.integer(center)
      hops      <- as.integer(hops)
      max_nodes <- as.integer(max_nodes)
      stopifnot(
        !is.na(center), !is.na(hops), hops >= 0L,
        !is.na(max_nodes), max_nodes >= 1L
      )
      private$check(private$send(list(
        type      = "GetSubgraph",
        center    = center,
        hops      = hops,
        max_nodes = max_nodes
      )))
    },

    #' @description
    #' Retrieve graph-wide statistics: node and edge counts, per-label
    #' node counts, and vector-index information when available.
    #'
    #' @return A named list of statistics, including \code{total_nodes},
    #'   \code{total_edges}, and \code{labels} (a per-label node count).
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   stats <- client$graph_stats()
    #'   stats$total_nodes
    #'   client$disconnect()
    #' }
    #' }
    graph_stats = function() {
      private$assert_connected()
      private$check(private$send(list(type = "GraphStats")))
    },

    # ══════════════════════════════════════════════════════════
    # Graph Algorithms
    # ══════════════════════════════════════════════════════════

    #' @description
    #' Run the PageRank algorithm over the whole graph or a node subset.
    #'
    #' @param nodes Optional integer vector. Restrict the computation to
    #'   these node IDs. \code{NULL} (default) uses the whole graph.
    #' @param damping Numeric scalar. Damping factor. Default \code{0.85}.
    #' @param max_iterations Integer scalar. Maximum iterations.
    #'   Default \code{100L}.
    #' @param tolerance Numeric scalar. Convergence tolerance.
    #'   Default \code{1e-6}.
    #' @return A named list mapping node ID (a character key) to its
    #'   PageRank score.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   scores <- client$run_pagerank()
    #'   client$disconnect()
    #' }
    #' }
    run_pagerank = function(nodes = NULL, damping = 0.85,
                            max_iterations = 100L, tolerance = 1e-6) {
      private$assert_connected()
      stopifnot(
        is.numeric(damping), length(damping) == 1L,
        is.numeric(tolerance), length(tolerance) == 1L
      )
      max_iterations <- as.integer(max_iterations)
      stopifnot(!is.na(max_iterations), max_iterations >= 1L)
      req <- list(
        type           = "RunPageRank",
        damping        = damping,
        max_iterations = max_iterations,
        tolerance      = tolerance
      )
      if (!is.null(nodes)) req$nodes <- as.list(as.integer(nodes))
      data <- private$check(private$send(req))
      data$scores
    },

    #' @description
    #' Run Louvain community detection over the whole graph or a subset.
    #'
    #' @param nodes Optional integer vector. Restrict the computation to
    #'   these node IDs. \code{NULL} (default) uses the whole graph.
    #' @return A list with \code{communities} (a named list mapping node ID
    #'   to community index) and \code{num_communities}.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   res <- client$run_louvain()
    #'   res$num_communities
    #'   client$disconnect()
    #' }
    #' }
    run_louvain = function(nodes = NULL) {
      private$assert_connected()
      req <- list(type = "RunLouvain")
      if (!is.null(nodes)) req$nodes <- as.list(as.integer(nodes))
      private$check(private$send(req))
    },

    #' @description
    #' Find connected components of the whole graph or a node subset.
    #'
    #' @param nodes Optional integer vector. Restrict the computation to
    #'   these node IDs. \code{NULL} (default) uses the whole graph.
    #' @param strong Logical scalar. If \code{TRUE}, compute strongly
    #'   connected components; otherwise weakly connected. Default
    #'   \code{FALSE}.
    #' @return A list with \code{components} (a list of integer vectors of
    #'   node IDs) and \code{count}.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   cc <- client$run_connected_components()
    #'   cc$count
    #'   client$disconnect()
    #' }
    #' }
    run_connected_components = function(nodes = NULL, strong = FALSE) {
      private$assert_connected()
      stopifnot(is.logical(strong), length(strong) == 1L)
      req <- list(type = "RunConnectedComponents", strong = strong)
      if (!is.null(nodes)) req$nodes <- as.list(as.integer(nodes))
      private$check(private$send(req))
    },

    #' @description
    #' Compute degree centrality for the whole graph or a node subset.
    #'
    #' @param nodes Optional integer vector. Restrict the computation to
    #'   these node IDs. \code{NULL} (default) uses the whole graph.
    #' @param direction Character scalar. One of \code{"outgoing"}
    #'   (default), \code{"incoming"}, or \code{"both"}.
    #' @return A named list mapping node ID (a character key) to its
    #'   degree-centrality score.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   scores <- client$run_degree_centrality(direction = "both")
    #'   client$disconnect()
    #' }
    #' }
    run_degree_centrality = function(nodes = NULL, direction = "outgoing") {
      private$assert_connected()
      stopifnot(
        is.character(direction), length(direction) == 1L,
        direction %in% c("outgoing", "incoming", "both")
      )
      req <- list(type = "RunDegreeCentrality", direction = direction)
      if (!is.null(nodes)) req$nodes <- as.list(as.integer(nodes))
      data <- private$check(private$send(req))
      data$scores
    },

    #' @description
    #' Compute betweenness centrality for the whole graph or a node subset.
    #'
    #' @param nodes Optional integer vector. Restrict the computation to
    #'   these node IDs. \code{NULL} (default) uses the whole graph.
    #' @return A named list mapping node ID (a character key) to its
    #'   betweenness-centrality score.
    #'
    #' @examples
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   scores <- client$run_betweenness_centrality()
    #'   client$disconnect()
    #' }
    #' }
    run_betweenness_centrality = function(nodes = NULL) {
      private$assert_connected()
      req <- list(type = "RunBetweennessCentrality")
      if (!is.null(nodes)) req$nodes <- as.list(as.integer(nodes))
      data <- private$check(private$send(req))
      data$scores
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   ids <- client$create_nodes(list(
    #'     list(labels = "Person", properties = list(name = "Alice")),
    #'     list(labels = "Person", properties = list(name = "Bob"))
    #'   ))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   eids <- client$create_edges(list(
    #'     list(source = a, target = b, edge_type = "KNOWS"),
    #'     list(source = b, target = b, edge_type = "FOLLOWS", weight = 0.5)
    #'   ))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   deleted <- client$delete_nodes(c(a, b))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   deleted <- client$delete_edges(e)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   df <- data.frame(
    #'     label = c("Person", "Person"),
    #'     name  = c("Alice", "Bob"),
    #'     age   = c(30, 25),
    #'     stringsAsFactors = FALSE
    #'   )
    #'   ids <- client$import_nodes_df(df)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   edf <- data.frame(
    #'     source = c(a, b),
    #'     target = c(b, a),
    #'     type   = c("KNOWS", "FOLLOWS"),
    #'     stringsAsFactors = FALSE
    #'   )
    #'   eids <- client$import_edges_df(edf)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   df <- client$export_nodes_df(c(a, b))
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   bfs_df <- client$export_bfs_df(a, max_depth = b)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   dim <- client$ping()$vector_dim
    #'   results <- client$vector_search(rep(0.1, dim), k = 5L)
    #'   df <- client$results_to_dataframe(results)
    #'   client$disconnect()
    #' }
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
    #' \donttest{
    #' if (astraea_server_available()) {
    #'   client <- astraea_connect()
    #'   a <- client$create_node(c("Person"), list(name = "Alice"))
    #'   b <- client$create_node(c("Person"), list(name = "Bob"))
    #'   df <- client$nodes_to_dataframe(c(a, b))
    #'   df$labels[[1]]
    #'   client$disconnect()
    #' }
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

