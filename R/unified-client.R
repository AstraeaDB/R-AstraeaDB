#' @title UnifiedClient
#'
#' @description
#' An R6 class that provides a unified interface to AstraeaDB by automatically
#' selecting the best available transport. It always creates a JSON/TCP client
#' (\code{\link{AstraeaClient}}) and optionally creates an Arrow Flight client
#' (\code{\link{ArrowClient}}) when the \pkg{arrow} package is installed.
#'
#' @details
#' The UnifiedClient delegates all CRUD operations (node, edge, traversal,
#' vector search, etc.) to the JSON/TCP client. For GQL query execution,
#' it prefers the Arrow Flight transport when available, as Arrow provides
#' more efficient columnar data transfer for analytical workloads. If Arrow
#' is not available or its connection fails, queries transparently fall back
#' to the JSON/TCP transport.
#'
#' Typical usage:
#' \enumerate{
#'   \item Create a \code{UnifiedClient} instance.
#'   \item Call \code{$connect()} to establish connections.
#'   \item Use CRUD methods (\code{$create_node()}, \code{$get_node()}, etc.)
#'         and query methods (\code{$query()}, \code{$query_df()}).
#'   \item Call \code{$disconnect()} when finished.
#' }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Basic usage
#' client <- UnifiedClient$new()
#' client$connect()
#'
#' # CRUD operations use JSON/TCP
#' node_id <- client$create_node(list("Person"), list(name = "Alice", age = 30))
#' node <- client$get_node(node_id)
#'
#' # Queries use Arrow Flight if available, otherwise JSON/TCP
#' df <- client$query_df("MATCH (n:Person) RETURN n.name, n.age")
#'
#' # Check transport status
#' client$is_arrow_enabled()
#'
#' client$disconnect()
#'
#' # With authentication
#' client <- UnifiedClient$new(auth_token = "my-secret-token")
#' client$connect()
#'
#' # Custom Flight URI
#' client <- UnifiedClient$new(
#'   host = "db.example.com",
#'   port = 7687L,
#'   flight_uri = "grpc://db.example.com:7689"
#' )
#' client$connect()
#' }
#'
#' @importFrom R6 R6Class
UnifiedClient <- R6::R6Class(
  classname = "UnifiedClient",
  cloneable = FALSE,

  public = list(

    #' @field json_client An \code{\link{AstraeaClient}} instance for JSON/TCP
    #'   communication. Always available.
    json_client = NULL,

    #' @field arrow_client An \code{\link{ArrowClient}} instance for Arrow
    #'   Flight communication, or \code{NULL} if the \pkg{arrow} package is not
    #'   installed or initialization failed.
    arrow_client = NULL,

    #' @field use_arrow Logical. Whether Arrow Flight transport is active and
    #'   should be used for queries.
    use_arrow = FALSE,

    #' @description
    #' Create a new UnifiedClient.
    #'
    #' Always creates a JSON/TCP client. If the \pkg{arrow} package is
    #' available, also creates an Arrow Flight client. Arrow initialization
    #' failure is non-fatal and results in a message.
    #'
    #' @param host Character. Server hostname for JSON/TCP transport.
    #'   Defaults to \code{"127.0.0.1"}.
    #' @param port Integer. Server port for JSON/TCP transport.
    #'   Defaults to \code{7687L}.
    #' @param flight_uri Character or \code{NULL}. Arrow Flight gRPC URI.
    #'   If \code{NULL}, constructed automatically as
    #'   \code{sprintf("grpc://\%s:7689", host)}.
    #' @param auth_token Character or \code{NULL}. Authentication token for
    #'   the JSON/TCP client.
    #'
    #' @return A new \code{UnifiedClient} object.
    initialize = function(host = "127.0.0.1", port = 7687L,
                          flight_uri = NULL, auth_token = NULL) {
      # Always create JSON client
      self$json_client <- AstraeaClient$new(
        host = host,
        port = port,
        auth_token = auth_token
      )

      # Try to create Arrow client if the package is available
      self$arrow_client <- NULL
      self$use_arrow <- FALSE

      if (requireNamespace("arrow", quietly = TRUE)) {
        resolved_uri <- flight_uri %||% sprintf("grpc://%s:7689", host)
        tryCatch(
          {
            self$arrow_client <- ArrowClient$new(uri = resolved_uri)
            self$use_arrow <- TRUE
          },
          error = function(e) {
            message("Arrow Flight client initialization failed, using JSON/TCP: ",
                    conditionMessage(e))
          }
        )
      }
    },

    #' @description
    #' Connect to AstraeaDB server(s).
    #'
    #' Connects the JSON/TCP client. If Arrow Flight is enabled, also attempts
    #' to connect the Arrow client. Arrow connection failure is non-fatal and
    #' results in a fallback to JSON/TCP with a message.
    #'
    #' @return Invisibly returns \code{self} for method chaining.
    connect = function() {
      self$json_client$connect()

      if (self$use_arrow && !is.null(self$arrow_client)) {
        tryCatch(
          {
            self$arrow_client$connect()
          },
          error = function(e) {
            self$use_arrow <- FALSE
            message("Arrow Flight connection failed, using JSON/TCP only: ",
                    conditionMessage(e))
          }
        )
      }
      invisible(self)
    },

    #' @description
    #' Disconnect all client connections.
    #'
    #' Closes the JSON/TCP connection and, if active, the Arrow Flight
    #' connection.
    #'
    #' @return Invisibly returns \code{self} for method chaining.
    disconnect = function() {
      self$json_client$disconnect()
      if (!is.null(self$arrow_client)) {
        self$arrow_client$disconnect()
      }
      invisible(self)
    },

    # -- Node Operations ---------------------------------------------------

    #' @description
    #' Create a node. Delegates to the JSON/TCP client.
    #'
    #' @param labels List of character labels for the node.
    #' @param properties Named list of node properties.
    #' @param embedding Optional numeric vector for vector similarity search.
    #'
    #' @return Integer. The ID of the newly created node.
    create_node = function(labels, properties, embedding = NULL) {
      self$json_client$create_node(labels, properties, embedding)
    },

    #' @description
    #' Get a node by ID. Delegates to the JSON/TCP client.
    #'
    #' @param node_id Integer. The node ID.
    #'
    #' @return A list containing node data (labels, properties, etc.).
    get_node = function(node_id) {
      self$json_client$get_node(node_id)
    },

    #' @description
    #' Update a node's properties. Delegates to the JSON/TCP client.
    #'
    #' @param node_id Integer. The node ID.
    #' @param properties Named list of properties to merge.
    #'
    #' @return The server response data.
    update_node = function(node_id, properties) {
      self$json_client$update_node(node_id, properties)
    },

    #' @description
    #' Delete a node and its connected edges. Delegates to the JSON/TCP client.
    #'
    #' @param node_id Integer. The node ID.
    #'
    #' @return The server response data.
    delete_node = function(node_id) {
      self$json_client$delete_node(node_id)
    },

    # -- Edge Operations ---------------------------------------------------

    #' @description
    #' Create an edge. Delegates to the JSON/TCP client.
    #'
    #' @param source Integer. Source node ID.
    #' @param target Integer. Target node ID.
    #' @param edge_type Character. The edge type label.
    #' @param properties Named list of edge properties.
    #' @param weight Numeric. Edge weight. Defaults to \code{1.0}.
    #' @param valid_from Numeric or \code{NULL}. Temporal start (ms epoch).
    #' @param valid_to Numeric or \code{NULL}. Temporal end (ms epoch).
    #'
    #' @return Integer. The ID of the newly created edge.
    create_edge = function(source, target, edge_type,
                           properties = list(), weight = 1.0,
                           valid_from = NULL, valid_to = NULL) {
      self$json_client$create_edge(source, target, edge_type,
                                   properties, weight,
                                   valid_from, valid_to)
    },

    #' @description
    #' Get an edge by ID. Delegates to the JSON/TCP client.
    #'
    #' @param edge_id Integer. The edge ID.
    #'
    #' @return A list containing edge data.
    get_edge = function(edge_id) {
      self$json_client$get_edge(edge_id)
    },

    #' @description
    #' Update an edge's properties. Delegates to the JSON/TCP client.
    #'
    #' @param edge_id Integer. The edge ID.
    #' @param properties Named list of properties to merge.
    #'
    #' @return The server response data.
    update_edge = function(edge_id, properties) {
      self$json_client$update_edge(edge_id, properties)
    },

    #' @description
    #' Delete an edge. Delegates to the JSON/TCP client.
    #'
    #' @param edge_id Integer. The edge ID.
    #'
    #' @return The server response data.
    delete_edge = function(edge_id) {
      self$json_client$delete_edge(edge_id)
    },

    # -- Traversal Operations ----------------------------------------------

    #' @description
    #' Get neighbors of a node. Delegates to the JSON/TCP client.
    #'
    #' @param node_id Integer. The node ID.
    #' @param direction Character. One of \code{"outgoing"}, \code{"incoming"},
    #'   or \code{"both"}. Defaults to \code{"outgoing"}.
    #' @param edge_type Character or \code{NULL}. Filter by edge type.
    #'
    #' @return A list of neighbor entries.
    neighbors = function(node_id, direction = "outgoing", edge_type = NULL) {
      self$json_client$neighbors(node_id, direction, edge_type)
    },

    #' @description
    #' Breadth-first search from a start node. Delegates to the JSON/TCP client.
    #'
    #' @param start Integer. Starting node ID.
    #' @param max_depth Integer. Maximum traversal depth. Defaults to \code{3L}.
    #'
    #' @return A list of entries with \code{node_id} and \code{depth}.
    bfs = function(start, max_depth = 3L) {
      self$json_client$bfs(start, max_depth)
    },

    #' @description
    #' Find the shortest path between two nodes. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param from_node Integer. Source node ID.
    #' @param to_node Integer. Target node ID.
    #' @param weighted Logical. Use edge weights? Defaults to \code{FALSE}.
    #'
    #' @return A list with path information.
    shortest_path = function(from_node, to_node, weighted = FALSE) {
      self$json_client$shortest_path(from_node, to_node, weighted)
    },

    # -- Temporal Operations -----------------------------------------------

    #' @description
    #' Get neighbors at a specific point in time. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param node_id Integer. The node ID.
    #' @param direction Character. Direction filter. Defaults to
    #'   \code{"outgoing"}.
    #' @param timestamp Numeric. Point-in-time timestamp (ms epoch).
    #' @param edge_type Character or \code{NULL}. Filter by edge type.
    #'
    #' @return A list of neighbor entries valid at the given timestamp.
    neighbors_at = function(node_id, direction = "outgoing", timestamp,
                            edge_type = NULL) {
      self$json_client$neighbors_at(node_id, direction, timestamp, edge_type)
    },

    #' @description
    #' BFS traversal at a specific point in time. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param start Integer. Starting node ID.
    #' @param max_depth Integer. Maximum depth. Defaults to \code{3L}.
    #' @param timestamp Numeric. Point-in-time timestamp (ms epoch).
    #'
    #' @return A list of entries with \code{node_id} and \code{depth}.
    bfs_at = function(start, max_depth = 3L, timestamp) {
      self$json_client$bfs_at(start, max_depth, timestamp)
    },

    #' @description
    #' Find shortest path at a specific point in time. Delegates to the
    #' JSON/TCP client.
    #'
    #' @param from_node Integer. Source node ID.
    #' @param to_node Integer. Target node ID.
    #' @param timestamp Numeric. Point-in-time timestamp (ms epoch).
    #' @param weighted Logical. Use edge weights? Defaults to \code{FALSE}.
    #'
    #' @return A list with path information.
    shortest_path_at = function(from_node, to_node, timestamp,
                                weighted = FALSE) {
      self$json_client$shortest_path_at(from_node, to_node, timestamp, weighted)
    },

    # -- Vector / Semantic Search ------------------------------------------

    #' @description
    #' k-nearest neighbor vector search. Delegates to the JSON/TCP client.
    #'
    #' @param query_vector Numeric vector. The query embedding.
    #' @param k Integer. Number of results. Defaults to \code{10L}.
    #'
    #' @return A list of search results with \code{node_id} and
    #'   \code{similarity}.
    vector_search = function(query_vector, k = 10L) {
      self$json_client$vector_search(query_vector, k)
    },

    #' @description
    #' Hybrid graph-vector search. Delegates to the JSON/TCP client.
    #'
    #' @param anchor Integer. Anchor node ID for graph proximity.
    #' @param query_vector Numeric vector. Query embedding for similarity.
    #' @param max_hops Integer. Maximum graph hops. Defaults to \code{3L}.
    #' @param k Integer. Number of results. Defaults to \code{10L}.
    #' @param alpha Numeric. Balance between graph (0) and vector (1).
    #'   Defaults to \code{0.5}.
    #'
    #' @return A list of search results.
    hybrid_search = function(anchor, query_vector, max_hops = 3L,
                             k = 10L, alpha = 0.5) {
      self$json_client$hybrid_search(anchor, query_vector, max_hops, k, alpha)
    },

    #' @description
    #' Get neighbors ranked by semantic similarity. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param node_id Integer. The node ID.
    #' @param concept Numeric vector. Concept embedding for ranking.
    #' @param direction Character. Direction filter. Defaults to
    #'   \code{"outgoing"}.
    #' @param k Integer. Number of results. Defaults to \code{10L}.
    #'
    #' @return A list of neighbor entries with similarity scores.
    semantic_neighbors = function(node_id, concept, direction = "outgoing",
                                  k = 10L) {
      self$json_client$semantic_neighbors(node_id, concept, direction, k)
    },

    #' @description
    #' Greedy semantic walk following edges most similar to a concept.
    #' Delegates to the JSON/TCP client.
    #'
    #' @param start Integer. Starting node ID.
    #' @param concept Numeric vector. Concept embedding for guidance.
    #' @param max_hops Integer. Maximum walk length. Defaults to \code{3L}.
    #'
    #' @return A list representing the walk path.
    semantic_walk = function(start, concept, max_hops = 3L) {
      self$json_client$semantic_walk(start, concept, max_hops)
    },

    # -- GraphRAG ----------------------------------------------------------

    #' @description
    #' Extract a subgraph centered on a node. Delegates to the JSON/TCP client.
    #'
    #' @param center Integer. Center node ID.
    #' @param hops Integer. Radius in hops. Defaults to \code{2L}.
    #' @param max_nodes Integer. Maximum nodes to return. Defaults to
    #'   \code{50L}.
    #' @param format Character. Output format: \code{"structured"},
    #'   \code{"prose"}, \code{"triples"}, or \code{"json"}.
    #'   Defaults to \code{"structured"}.
    #'
    #' @return A list with subgraph data and linearized text.
    extract_subgraph = function(center, hops = 2L, max_nodes = 50L,
                                format = "structured") {
      self$json_client$extract_subgraph(center, hops, max_nodes, format)
    },

    #' @description
    #' Execute a GraphRAG query. Delegates to the JSON/TCP client.
    #'
    #' @param question Character. The question to answer.
    #' @param anchor Integer or \code{NULL}. Anchor node ID.
    #' @param question_embedding Numeric vector or \code{NULL}. Embedding of
    #'   the question.
    #' @param hops Integer. Subgraph radius. Defaults to \code{2L}.
    #' @param max_nodes Integer. Maximum subgraph nodes. Defaults to
    #'   \code{50L}.
    #' @param format Character. Linearization format. Defaults to
    #'   \code{"structured"}.
    #'
    #' @return A list with the LLM-generated answer and context.
    graph_rag = function(question, anchor = NULL, question_embedding = NULL,
                         hops = 2L, max_nodes = 50L, format = "structured") {
      self$json_client$graph_rag(question, anchor, question_embedding,
                                 hops, max_nodes, format)
    },

    # -- Anomaly Detection -------------------------------------------------

    #' @description
    #' Check anomaly status of an entity. Delegates to the JSON/TCP client.
    #'
    #' @param entity_id Integer. The node or edge ID.
    #' @param is_node Logical. Check a node (\code{TRUE}) or edge
    #'   (\code{FALSE}). Defaults to \code{TRUE}.
    #'
    #' @return A list with anomaly score information.
    anomaly_check = function(entity_id, is_node = TRUE) {
      self$json_client$anomaly_check(entity_id, is_node)
    },

    #' @description
    #' Get detailed anomaly statistics. Delegates to the JSON/TCP client.
    #'
    #' @param entity_id Integer. The node or edge ID.
    #' @param is_node Logical. Query a node (\code{TRUE}) or edge
    #'   (\code{FALSE}). Defaults to \code{TRUE}.
    #'
    #' @return A list with detailed anomaly statistics.
    anomaly_stats = function(entity_id, is_node = TRUE) {
      self$json_client$anomaly_stats(entity_id, is_node)
    },

    #' @description
    #' Get all active anomaly alerts. Delegates to the JSON/TCP client.
    #'
    #' @return A list of alert entries.
    anomaly_alerts = function() {
      self$json_client$anomaly_alerts()
    },

    # -- Batch Operations --------------------------------------------------

    #' @description
    #' Create multiple nodes. Delegates to the JSON/TCP client.
    #'
    #' @param nodes_list A list of lists, each with \code{labels},
    #'   \code{properties}, and optionally \code{embedding}.
    #'
    #' @return An integer vector of created node IDs.
    create_nodes = function(nodes_list) {
      self$json_client$create_nodes(nodes_list)
    },

    #' @description
    #' Create multiple edges. Delegates to the JSON/TCP client.
    #'
    #' @param edges_list A list of lists, each with \code{source},
    #'   \code{target}, \code{edge_type}, and optionally \code{properties},
    #'   \code{weight}, \code{valid_from}, \code{valid_to}.
    #'
    #' @return An integer vector of created edge IDs.
    create_edges = function(edges_list) {
      self$json_client$create_edges(edges_list)
    },

    #' @description
    #' Delete multiple nodes. Delegates to the JSON/TCP client.
    #'
    #' @param node_ids Integer vector of node IDs to delete.
    #'
    #' @return Integer. Number of successfully deleted nodes.
    delete_nodes = function(node_ids) {
      self$json_client$delete_nodes(node_ids)
    },

    #' @description
    #' Delete multiple edges. Delegates to the JSON/TCP client.
    #'
    #' @param edge_ids Integer vector of edge IDs to delete.
    #'
    #' @return Integer. Number of successfully deleted edges.
    delete_edges = function(edge_ids) {
      self$json_client$delete_edges(edge_ids)
    },

    # -- Data Frame Import/Export ------------------------------------------

    #' @description
    #' Import nodes from a data.frame. Delegates to the JSON/TCP client.
    #'
    #' @param df A data.frame containing node data.
    #' @param label_col Character. Column name for labels.
    #'   Defaults to \code{"label"}.
    #' @param id_col Character or \code{NULL}. Column for external IDs.
    #' @param embedding_cols Character vector or \code{NULL}. Columns for
    #'   embedding values.
    #'
    #' @return An integer vector of created node IDs.
    import_nodes_df = function(df, label_col = "label", id_col = NULL,
                               embedding_cols = NULL) {
      self$json_client$import_nodes_df(df, label_col, id_col, embedding_cols)
    },

    #' @description
    #' Import edges from a data.frame. Delegates to the JSON/TCP client.
    #'
    #' @param df A data.frame containing edge data.
    #' @param source_col Character. Column for source node IDs.
    #'   Defaults to \code{"source"}.
    #' @param target_col Character. Column for target node IDs.
    #'   Defaults to \code{"target"}.
    #' @param type_col Character. Column for edge types.
    #'   Defaults to \code{"type"}.
    #' @param weight_col Character or \code{NULL}. Column for weights.
    #' @param valid_from_col Character or \code{NULL}. Column for temporal
    #'   start.
    #' @param valid_to_col Character or \code{NULL}. Column for temporal end.
    #'
    #' @return An integer vector of created edge IDs.
    import_edges_df = function(df, source_col = "source",
                               target_col = "target", type_col = "type",
                               weight_col = NULL, valid_from_col = NULL,
                               valid_to_col = NULL) {
      self$json_client$import_edges_df(df, source_col, target_col, type_col,
                                       weight_col, valid_from_col,
                                       valid_to_col)
    },

    #' @description
    #' Export nodes to a data.frame. Delegates to the JSON/TCP client.
    #'
    #' @param node_ids Integer vector of node IDs.
    #'
    #' @return A data.frame with columns for node ID, labels, and properties.
    export_nodes_df = function(node_ids) {
      self$json_client$export_nodes_df(node_ids)
    },

    #' @description
    #' Run BFS and return results as a data.frame. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param start Integer. Starting node ID.
    #' @param max_depth Integer. Maximum depth. Defaults to \code{3L}.
    #'
    #' @return A data.frame with BFS results including node details.
    export_bfs_df = function(start, max_depth = 3L) {
      self$json_client$export_bfs_df(start, max_depth)
    },

    # -- Utility Functions --------------------------------------------------

    #' @description
    #' Convert search results to a data.frame. Delegates to the JSON/TCP
    #' client.
    #'
    #' @param results A list of result entries.
    #'
    #' @return A data.frame with one row per result.
    results_to_dataframe = function(results) {
      self$json_client$results_to_dataframe(results)
    },

    #' @description
    #' Fetch nodes by ID and return as a data.frame. Delegates to the
    #' JSON/TCP client.
    #'
    #' @param node_ids Integer vector of node IDs.
    #'
    #' @return A data.frame with columns for ID, labels, and properties.
    nodes_to_dataframe = function(node_ids) {
      self$json_client$nodes_to_dataframe(node_ids)
    },

    # -- Health Check ------------------------------------------------------

    #' @description
    #' Health check. Delegates to the JSON/TCP client.
    #'
    #' @return A list with server status information.
    ping = function() {
      self$json_client$ping()
    },

    # -- Query Operations (Arrow-aware) ------------------------------------

    #' @description
    #' Execute a GQL query.
    #'
    #' Uses Arrow Flight transport if available for higher performance.
    #' Falls back to JSON/TCP transport otherwise.
    #'
    #' @param gql Character. A GQL query string.
    #'
    #' @return Query results. An Arrow Table when using Arrow transport, or a
    #'   list when using JSON/TCP.
    #'
    #' @examples
    #' \dontrun{
    #' client <- UnifiedClient$new()
    #' client$connect()
    #' result <- client$query("MATCH (n:Person) RETURN n.name")
    #' client$disconnect()
    #' }
    query = function(gql) {
      if (self$use_arrow && !is.null(self$arrow_client)) {
        self$arrow_client$query(gql)
      } else {
        self$json_client$query(gql)
      }
    },

    #' @description
    #' Execute a GQL query and return a data.frame.
    #'
    #' Uses Arrow Flight transport if available. Falls back to JSON/TCP,
    #' converting the result to a data.frame.
    #'
    #' @param gql Character. A GQL query string.
    #'
    #' @return A data.frame containing the query results.
    #'
    #' @examples
    #' \dontrun{
    #' client <- UnifiedClient$new()
    #' client$connect()
    #' df <- client$query_df("MATCH (n:Person) RETURN n.name, n.age")
    #' client$disconnect()
    #' }
    query_df = function(gql) {
      if (self$use_arrow && !is.null(self$arrow_client)) {
        self$arrow_client$query_df(gql)
      } else {
        result <- self$json_client$query(gql)
        if (!is.null(result$rows)) {
          do.call(rbind, lapply(result$rows, as.data.frame))
        } else {
          data.frame()
        }
      }
    },

    #' @description
    #' Check whether Arrow Flight transport is currently active.
    #'
    #' @return Logical. \code{TRUE} if Arrow Flight is enabled and connected,
    #'   \code{FALSE} otherwise.
    is_arrow_enabled = function() {
      self$use_arrow && !is.null(self$arrow_client)
    },

    #' @description
    #' Print a summary of the UnifiedClient.
    #'
    #' @param ... Ignored. Present for compatibility with the generic.
    #'
    #' @return Invisibly returns \code{self}.
    print = function(...) {
      cat("<UnifiedClient>\n")
      cat("  JSON/TCP:  connected to ",
          self$json_client$host, ":", self$json_client$port, "\n", sep = "")
      if (self$is_arrow_enabled()) {
        cat("  Arrow:     enabled (", self$arrow_client$uri, ")\n", sep = "")
      } else {
        cat("  Arrow:     disabled\n")
      }
      invisible(self)
    }
  )
)
