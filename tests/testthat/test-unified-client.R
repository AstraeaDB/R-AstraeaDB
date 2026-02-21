# Tests for UnifiedClient

test_that("UnifiedClient can be created", {
  client <- UnifiedClient$new()
  expect_s3_class(client, "UnifiedClient")
  expect_s3_class(client$json_client, "AstraeaClient")
})

test_that("UnifiedClient reports arrow status", {
  client <- UnifiedClient$new()
  result <- client$is_arrow_enabled()
  expect_type(result, "logical")
  expect_length(result, 1L)
})

test_that("UnifiedClient print works", {
  client <- UnifiedClient$new()
  expect_output(print(client), "UnifiedClient")
  expect_output(print(client), "JSON/TCP")
})

test_that("UnifiedClient delegates to json_client for host and port", {
  client <- UnifiedClient$new(host = "10.0.0.5", port = 8888L)
  expect_equal(client$json_client$host, "10.0.0.5")
  expect_equal(client$json_client$port, 8888L)
})

test_that("UnifiedClient passes auth_token to json_client", {
  client <- UnifiedClient$new(auth_token = "secret-token")
  expect_equal(client$json_client$auth_token, "secret-token")
})

test_that("UnifiedClient disconnect on unconnected client is safe", {
  client <- UnifiedClient$new()
  expect_no_error(client$disconnect())
})
