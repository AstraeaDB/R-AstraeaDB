#' @title ArrowClient
#'
#' @description
#' An R6 class providing a high-performance client for AstraeaDB using the
#' Apache Arrow Flight protocol. Arrow Flight enables efficient columnar data
#' transfer, making it well-suited for bulk queries and analytical workloads.
#'
#' The \pkg{arrow} package is optional and listed in \code{Suggests}. This
#' client checks for its availability at initialization and provides an
#' informative error message if \pkg{arrow} is not installed.
#'
#' @details
#' Arrow Flight is a gRPC-based protocol for high-performance data transport.
#' When the AstraeaDB server exposes a Flight endpoint, this client can execute
#' GQL queries and receive results as Apache Arrow Tables or record batches,
#' which are more efficient than JSON for large result sets.
#'
#' Typical usage:
#' \enumerate{
#'   \item Create an \code{ArrowClient} instance with the Flight URI.
#'   \item Call \code{$connect()} to establish the connection.
#'   \item Execute queries with \code{$query()} (returns Arrow Table) or
#'         \code{$query_df()} (returns data.frame).
#'   \item Call \code{$disconnect()} when finished.
#' }
#'
#' For most users, the \code{\link{UnifiedClient}} is recommended as it
#' automatically selects the best transport available.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Requires the arrow package: install.packages("arrow")
#' client <- ArrowClient$new("grpc://localhost:7689")
#' client$connect()
#'
#' # Execute a GQL query and get an Arrow Table
#' table <- client$query("MATCH (n:Person) RETURN n.name, n.age")
#'
#' # Or get a data.frame directly
#' df <- client$query_df("MATCH (n:Person) RETURN n.name, n.age")
#'
#' # Process large results in batches
#' client$query_batches(
#'   "MATCH (n) RETURN n",
#'   callback = function(batch) {
#'     cat("Received batch with", nrow(batch), "rows\n")
#'   }
#' )
#'
#' # List available flights
#' flights <- client$list_flights()
#'
#' client$disconnect()
#' }
#'
#' @importFrom R6 R6Class
ArrowClient <- R6::R6Class(
  classname = "ArrowClient",
  cloneable = FALSE,

  public = list(

    #' @field uri Character. The Arrow Flight server URI.
    #'   Defaults to \code{"grpc://localhost:7689"}.
    uri = NULL,

    #' @field client The Arrow Flight client connection object, or \code{NULL}
    #'   when disconnected.
    client = NULL,

    #' @description
    #' Create a new ArrowClient.
    #'
    #' Checks that the \pkg{arrow} package is installed and available. If not,
    #' an informative error is raised directing the user to install it.
    #'
    #' @param uri Character. The Arrow Flight gRPC URI.
    #'   Defaults to \code{"grpc://localhost:7689"}.
    #'
    #' @return A new \code{ArrowClient} object.
    initialize = function(uri = "grpc://localhost:7689") {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop(
          "The 'arrow' package is required for ArrowClient but is not installed.\n",
          "Install it with: install.packages(\"arrow\")\n",
          "Alternatively, use AstraeaClient for JSON/TCP transport.",
          call. = FALSE
        )
      }
      self$uri <- uri
      self$client <- NULL
    },

    #' @description
    #' Connect to the Arrow Flight server.
    #'
    #' Establishes a gRPC connection to the AstraeaDB Flight endpoint
    #' specified by \code{uri}.
    #'
    #' @return Invisibly returns \code{self} for method chaining.
    connect = function() {
      self$client <- arrow::flight_connect(self$uri)
      invisible(self)
    },

    #' @description
    #' Disconnect from the Arrow Flight server.
    #'
    #' Closes the connection and sets the client to \code{NULL}.
    #'
    #' @return Invisibly returns \code{self} for method chaining.
    disconnect = function() {
      if (!is.null(self$client)) {
        self$client <- NULL
      }
      invisible(self)
    },

    #' @description
    #' Check whether the client is currently connected.
    #'
    #' @return Logical. \code{TRUE} if connected, \code{FALSE} otherwise.
    is_connected = function() {
      !is.null(self$client)
    },

    #' @description
    #' Execute a GQL query and return an Arrow Table.
    #'
    #' Sends the query as a Flight command descriptor, retrieves flight info,
    #' and fetches the data from the first endpoint.
    #'
    #' @param gql Character. A GQL query string.
    #'
    #' @return An Arrow Table containing the query results.
    #'
    #' @examples
    #' \dontrun{
    #' client <- ArrowClient$new()
    #' client$connect()
    #' table <- client$query("MATCH (n:Person) RETURN n.name")
    #' client$disconnect()
    #' }
    query = function(gql) {
      if (!self$is_connected()) {
        stop("Not connected. Call $connect() first.", call. = FALSE)
      }
      descriptor <- arrow::flight_descriptor_for_command(gql)
      info <- self$client$get_flight_info(descriptor)
      reader <- self$client$do_get(info$endpoints[[1]]$ticket)
      reader$read_all()
    },

    #' @description
    #' Execute a GQL query and return the result as a data.frame.
    #'
    #' This is a convenience wrapper around \code{$query()} that converts the
    #' Arrow Table to a base R data.frame.
    #'
    #' @param gql Character. A GQL query string.
    #'
    #' @return A data.frame containing the query results.
    #'
    #' @examples
    #' \dontrun{
    #' client <- ArrowClient$new()
    #' client$connect()
    #' df <- client$query_df("MATCH (n:Person) RETURN n.name, n.age")
    #' client$disconnect()
    #' }
    query_df = function(gql) {
      tbl <- self$query(gql)
      as.data.frame(tbl)
    },

    #' @description
    #' Execute a GQL query and process results in record batches.
    #'
    #' Instead of reading all results into memory at once, this method
    #' streams results as individual Arrow RecordBatch objects and passes
    #' each to the provided callback function. This is useful for processing
    #' large result sets with bounded memory usage.
    #'
    #' @param gql Character. A GQL query string.
    #' @param callback Function. A function that accepts a single argument
    #'   (an Arrow RecordBatch). Called once for each batch received.
    #'
    #' @return Invisibly returns \code{NULL}.
    #'
    #' @examples
    #' \dontrun{
    #' client <- ArrowClient$new()
    #' client$connect()
    #' client$query_batches(
    #'   "MATCH (n) RETURN n",
    #'   callback = function(batch) {
    #'     cat("Batch with", nrow(batch), "rows\n")
    #'   }
    #' )
    #' client$disconnect()
    #' }
    query_batches = function(gql, callback) {
      if (!self$is_connected()) {
        stop("Not connected. Call $connect() first.", call. = FALSE)
      }
      if (!is.function(callback)) {
        stop("'callback' must be a function.", call. = FALSE)
      }
      descriptor <- arrow::flight_descriptor_for_command(gql)
      info <- self$client$get_flight_info(descriptor)
      reader <- self$client$do_get(info$endpoints[[1]]$ticket)
      repeat {
        batch <- reader$read_next_batch()
        if (is.null(batch)) break
        callback(batch)
      }
      invisible(NULL)
    },

    #' @description
    #' List available flights on the server.
    #'
    #' Queries the Flight server for its list of available flight descriptors.
    #'
    #' @return A list of available flights as reported by the server.
    #'
    #' @examples
    #' \dontrun{
    #' client <- ArrowClient$new()
    #' client$connect()
    #' flights <- client$list_flights()
    #' client$disconnect()
    #' }
    list_flights = function() {
      if (!self$is_connected()) {
        stop("Not connected. Call $connect() first.", call. = FALSE)
      }
      self$client$list_flights()
    },

    #' @description
    #' Print a summary of the ArrowClient.
    #'
    #' @param ... Ignored. Present for compatibility with the generic.
    #'
    #' @return Invisibly returns \code{self}.
    print = function(...) {
      cat("<ArrowClient>\n")
      cat("  URI:       ", self$uri, "\n")
      cat("  Connected: ", self$is_connected(), "\n")
      invisible(self)
    }
  )
)

