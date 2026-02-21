test_that("classify_service_port: tier 1 - privileged port wins", {
  # dst < 1024, src ephemeral

  expect_equal(classify_service_port(52000, 443), 443L)
  # src < 1024, dst ephemeral

  expect_equal(classify_service_port(80, 52000), 80L)
  # both < 1024 -> falls through to tier 2/3
  expect_equal(classify_service_port(22, 80), 22L)
})

test_that("classify_service_port: tier 2 - known service port wins", {
  # 3306 (MySQL) is in the lookup, 52000 is not; both > 1024

  expect_equal(classify_service_port(3306, 52000), 3306L)
  expect_equal(classify_service_port(52000, 3306), 3306L)
})

test_that("classify_service_port: tier 3 - lower port fallback", {
  # Neither port is privileged or in the lookup table

  expect_equal(classify_service_port(40000, 41000), 40000L)
  expect_equal(classify_service_port(41000, 40000), 40000L)
})

test_that("classify_service_port: NA handling", {
  expect_equal(classify_service_port(NA, 80), NA_integer_)
  expect_equal(classify_service_port(80, NA), NA_integer_)
  expect_equal(classify_service_port(NA, NA), NA_integer_)
})

test_that("classify_service_port: vectorized input", {
  src <- c(52000, 80, 3306, 40000, NA)
  dst <- c(443, 52000, 52000, 41000, 80)
  result <- classify_service_port(src, dst)
  expect_equal(result, c(443L, 80L, 3306L, 40000L, NA_integer_))
  expect_length(result, 5L)
})

test_that("port_service_name: known ports", {
  expect_equal(port_service_name(443), "HTTPS")
  expect_equal(port_service_name(80), "HTTP")
  expect_equal(port_service_name(22), "SSH")
  expect_equal(port_service_name(3306), "MySQL")
})

test_that("port_service_name: unknown port returns NA", {
  expect_equal(port_service_name(99999), NA_character_)
})

test_that("port_service_name: vectorized input", {
  result <- port_service_name(c(80, 443, 99999, 53))
  expect_equal(result, c("HTTP", "HTTPS", NA_character_, "DNS"))
})
