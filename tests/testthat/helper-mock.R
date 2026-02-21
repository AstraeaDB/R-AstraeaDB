# Helper functions used across AstraeaDB test files.

#' Skip a test if no AstraeaDB server is available.
#'
#' Calls \code{astraea_server_available()} with the default host/port and
#' skips the current test if the server cannot be reached.
skip_if_no_server <- function() {
  skip_if_not(
    astraea_server_available(),
    "AstraeaDB server not available"
  )
}

#' Skip a test if the arrow package is not installed.
skip_if_no_arrow <- function() {
  skip_if_not_installed("arrow")
}
