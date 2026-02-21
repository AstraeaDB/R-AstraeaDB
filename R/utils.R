# Utility functions and convenience wrappers for AstraeaDB
# These provide simplified connection workflows and helper utilities.

#' Connect to AstraeaDB Server
#'
#' Creates an \code{\link{AstraeaClient}} and connects to the server in one
#' step. This is a convenience wrapper that instantiates the client, calls
#' \code{connect()}, and returns the connected client object.
#'
#' @param host Character. Server hostname. Default: \code{"127.0.0.1"}.
#' @param port Integer. Server port. Default: \code{7687L}.
#' @param auth_token Character or \code{NULL}. Optional authentication token.
#' @return A connected \code{\link{AstraeaClient}} object.
#' @export
#' @examples
#' \dontrun{
#' client <- astraea_connect()
#' client$ping()
#' client$disconnect()
#' }
astraea_connect <- function(host = "127.0.0.1", port = 7687L,
                            auth_token = NULL) {
  client <- AstraeaClient$new(host = host, port = port,
                              auth_token = auth_token)
  client$connect()
  client
}

#' Connect to AstraeaDB via Arrow Flight
#'
#' Creates an \code{\link{ArrowClient}} and connects to the Arrow Flight
#' server. This is a convenience wrapper that instantiates the client, calls
#' \code{connect()}, and returns the connected client object.
#'
#' The \pkg{arrow} package must be installed to use this function.
#'
#' @param uri Character. Flight server URI. Default:
#'   \code{"grpc://localhost:7689"}.
#' @return A connected \code{\link{ArrowClient}} object.
#' @export
#' @examples
#' \dontrun{
#' client <- astraea_arrow_connect()
#' result <- client$query_df("MATCH (n) RETURN n")
#' client$disconnect()
#' }
astraea_arrow_connect <- function(uri = "grpc://localhost:7689") {
  client <- ArrowClient$new(uri = uri)
  client$connect()
  client
}

#' Check if AstraeaDB Server is Available
#'
#' Tests whether an AstraeaDB server is reachable at the given host and port.
#' Useful for conditionally running examples and tests that require a live
#' server.
#'
#' @param host Character. Server hostname. Default: \code{"127.0.0.1"}.
#' @param port Integer. Server port. Default: \code{7687L}.
#' @param timeout Numeric. Connection timeout in seconds. Default: \code{2}.
#' @return Logical. \code{TRUE} if server is reachable, \code{FALSE} otherwise.
#' @export
#' @examples
#' astraea_server_available()
astraea_server_available <- function(host = "127.0.0.1", port = 7687L,
                                     timeout = 2) {
  tryCatch({
    suppressWarnings({
      con <- socketConnection(host = host, port = port, open = "r+b",
                              blocking = TRUE, timeout = timeout)
    })
    on.exit(close(con))
    TRUE
  }, warning = function(w) {
    FALSE
  }, error = function(e) {
    FALSE
  })
}

# Null-coalesce operator (internal use only)
# Returns `a` if not NULL, otherwise `b`.
`%||%` <- function(a, b) if (is.null(a)) b else a
