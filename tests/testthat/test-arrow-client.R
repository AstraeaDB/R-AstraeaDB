# Tests for ArrowClient

test_that("ArrowClient requires arrow package", {
  # If arrow is NOT installed, creating an ArrowClient should error.
  # If arrow IS installed, this test is not meaningful, so we skip.
  skip_if(requireNamespace("arrow", quietly = TRUE), "arrow is installed")
  expect_error(
    ArrowClient$new(),
    "arrow.*not installed"
  )
})

test_that("ArrowClient can be created with defaults", {
  skip_if_no_arrow()

  client <- ArrowClient$new()
  expect_s3_class(client, "ArrowClient")
  expect_equal(client$uri, "grpc://localhost:7689")
  expect_false(client$is_connected())
})

test_that("ArrowClient can be created with custom URI", {
  skip_if_no_arrow()

  client <- ArrowClient$new(uri = "grpc://10.0.0.1:9000")
  expect_equal(client$uri, "grpc://10.0.0.1:9000")
})

test_that("ArrowClient print works", {
  skip_if_no_arrow()

  client <- ArrowClient$new()
  expect_output(print(client), "ArrowClient")
  expect_output(print(client), "grpc://localhost:7689")
})

test_that("ArrowClient disconnect on unconnected client is safe", {
  skip_if_no_arrow()

  client <- ArrowClient$new()
  expect_no_error(client$disconnect())
})

test_that("ArrowClient query fails when not connected", {
  skip_if_no_arrow()

  client <- ArrowClient$new()
  expect_error(client$query("MATCH (n) RETURN n"), "Not connected")
})
