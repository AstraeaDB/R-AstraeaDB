# Tests for utility functions

test_that("astraea_server_available returns logical", {
  result <- astraea_server_available(timeout = 1)
  expect_type(result, "logical")
  expect_length(result, 1L)
})

test_that("astraea_server_available returns FALSE for bad port", {
  expect_false(astraea_server_available(port = 1L, timeout = 1))
})

test_that("astraea_connect fails without server", {
  expect_error(astraea_connect(port = 1L))
})

test_that("astraea_server_available accepts custom host", {
  result <- astraea_server_available(host = "192.0.2.1", port = 1L, timeout = 1)
  expect_type(result, "logical")
  expect_false(result)
})
