# Tests for AstraeaClient
# Unit tests run without a server; integration tests are skipped when no
# AstraeaDB server is reachable.

# ============================================================================
# Unit tests (no server required)
# ============================================================================

test_that("AstraeaClient can be created with defaults", {
  client <- AstraeaClient$new()
  expect_s3_class(client, "AstraeaClient")
  expect_equal(client$host, "127.0.0.1")
  expect_equal(client$port, 7687L)
  expect_false(client$is_connected())
})

test_that("AstraeaClient can be created with custom host and port", {
  client <- AstraeaClient$new(host = "192.168.1.1", port = 9999L)
  expect_equal(client$host, "192.168.1.1")
  expect_equal(client$port, 9999L)
})

test_that("AstraeaClient validates host parameter", {
  expect_error(AstraeaClient$new(host = 123))
  expect_error(AstraeaClient$new(host = c("a", "b")))
})

test_that("AstraeaClient validates port parameter", {
  expect_error(AstraeaClient$new(port = -1))
  expect_error(AstraeaClient$new(port = 99999))
})

test_that("AstraeaClient stops if not connected", {
  client <- AstraeaClient$new()
  expect_error(client$ping(), "Not connected")
})

test_that("AstraeaClient print method works", {
  client <- AstraeaClient$new()
  expect_output(print(client), "AstraeaClient")
})

test_that("disconnect on unconnected client is safe", {
  client <- AstraeaClient$new()
  expect_no_error(client$disconnect())
})

# ============================================================================
# Integration tests (require a running AstraeaDB server)
# ============================================================================

test_that("can connect and ping server", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  result <- client$ping()
  expect_type(result, "list")
})

test_that("can create and get a node", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  node_id <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = tag, value = 42)
  )
  withr::defer(tryCatch(client$delete_node(node_id), error = function(e) NULL))

  expect_type(node_id, "integer")
  expect_true(node_id > 0L)

  node <- client$get_node(node_id)
  expect_type(node, "list")
  expect_equal(node$properties$name, tag)
})

test_that("can create and get an edge", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_src"))
  )
  n2 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_tgt"))
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  edge_id <- client$create_edge(
    source = n1,
    target = n2,
    edge_type = "TEST_EDGE",
    properties = list(strength = 0.9)
  )
  expect_type(edge_id, "integer")
  expect_true(edge_id > 0L)

  edge <- client$get_edge(edge_id)
  expect_type(edge, "list")
})

test_that("can update node properties", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  node_id <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = tag, status = "initial")
  )
  withr::defer(tryCatch(client$delete_node(node_id), error = function(e) NULL))

  client$update_node(node_id, list(status = "updated", extra = TRUE))
  node <- client$get_node(node_id)
  expect_equal(node$properties$status, "updated")
})

test_that("can delete a node", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  node_id <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = tag)
  )

  client$delete_node(node_id)
  expect_error(client$get_node(node_id))
})

test_that("can find neighbors", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_a"))
  )
  n2 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_b"))
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  client$create_edge(
    source = n1,
    target = n2,
    edge_type = "KNOWS"
  )

  nbrs <- client$neighbors(n1, direction = "outgoing")
  expect_type(nbrs, "list")
})

test_that("can run BFS", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_start"))
  )
  n2 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_end"))
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  client$create_edge(source = n1, target = n2, edge_type = "LINKED")

  bfs_result <- client$bfs(n1, max_depth = 2L)
  expect_type(bfs_result, "list")
})

test_that("can find shortest path", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_from"))
  )
  n2 <- client$create_node(
    labels = c("TestNode"),
    properties = list(name = paste0(tag, "_to"))
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  client$create_edge(source = n1, target = n2, edge_type = "PATH_EDGE")

  sp <- client$shortest_path(n1, n2)
  expect_type(sp, "list")
})

test_that("can execute GQL query", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  node_id <- client$create_node(
    labels = c("TestGQL"),
    properties = list(name = tag)
  )
  withr::defer(tryCatch(client$delete_node(node_id), error = function(e) NULL))

  result <- client$query("MATCH (n:TestGQL) RETURN n.name")
  expect_type(result, "list")
})

test_that("can perform vector search", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  # A store pins its embedding width on first insert, so the test has to use
  # whatever the running server is configured for rather than a fixed length.
  dim <- client$ping()$vector_dim
  skip_if(is.null(dim), "server did not report vector_dim")
  vec <- rep(0.1, dim)

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  node_id <- client$create_node(
    labels = c("TestVector"),
    properties = list(name = tag),
    embedding = vec
  )
  withr::defer(tryCatch(client$delete_node(node_id), error = function(e) NULL))

  results <- client$vector_search(vec, k = 5L)
  expect_type(results, "list")
})

test_that("batch create_nodes works", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  nodes_list <- list(
    list(
      labels = "TestBatch",
      properties = list(name = paste0(tag, "_batch1"))
    ),
    list(
      labels = "TestBatch",
      properties = list(name = paste0(tag, "_batch2"))
    )
  )

  ids <- client$create_nodes(nodes_list)
  withr::defer({
    for (nid in ids) {
      tryCatch(client$delete_node(nid), error = function(e) NULL)
    }
  })

  expect_type(ids, "integer")
  expect_length(ids, 2L)
  expect_true(all(ids > 0L))
})

test_that("import_nodes_df works", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  df <- data.frame(
    label = c("TestImport", "TestImport"),
    name  = c(paste0(tag, "_imp1"), paste0(tag, "_imp2")),
    score = c(10, 20),
    stringsAsFactors = FALSE
  )

  ids <- client$import_nodes_df(df)
  withr::defer({
    for (nid in ids) {
      tryCatch(client$delete_node(nid), error = function(e) NULL)
    }
  })

  expect_type(ids, "integer")
  expect_length(ids, 2L)
})

test_that("export_nodes_df works", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestExport"),
    properties = list(name = paste0(tag, "_exp1"), val = 1)
  )
  n2 <- client$create_node(
    labels = c("TestExport"),
    properties = list(name = paste0(tag, "_exp2"), val = 2)
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  result_df <- client$export_nodes_df(c(n1, n2))
  expect_s3_class(result_df, "data.frame")
  expect_equal(nrow(result_df), 2L)
  expect_true("node_id" %in% names(result_df))
  expect_true("labels" %in% names(result_df))
})

# ============================================================================
# New commands: not-connected guards (unit tests)
# ============================================================================

test_that("traversal and lookup methods error when not connected", {
  client <- AstraeaClient$new()
  expect_error(client$dfs(1L), "Not connected")
  expect_error(client$dfs_at(1L, timestamp = 1), "Not connected")
  expect_error(client$find_by_label("Person"), "Not connected")
  expect_error(client$find_edge_by_type("KNOWS"), "Not connected")
  expect_error(client$delete_by_label("Tmp"), "Not connected")
})

test_that("subgraph and stats methods error when not connected", {
  client <- AstraeaClient$new()
  expect_error(client$get_subgraph(1L), "Not connected")
  expect_error(client$graph_stats(), "Not connected")
})

test_that("graph-algorithm methods error when not connected", {
  client <- AstraeaClient$new()
  expect_error(client$run_pagerank(), "Not connected")
  expect_error(client$run_louvain(), "Not connected")
  expect_error(client$run_connected_components(), "Not connected")
  expect_error(client$run_degree_centrality(), "Not connected")
  expect_error(client$run_betweenness_centrality(), "Not connected")
})

# ============================================================================
# Utility Functions (unit tests)
# ============================================================================

test_that("results_to_dataframe returns empty data.frame for empty list", {
  client <- AstraeaClient$new()
  df <- client$results_to_dataframe(list())
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 0L)
})

test_that("results_to_dataframe converts mock results", {
  client <- AstraeaClient$new()
  mock_results <- list(
    list(node_id = 1L, similarity = 0.95),
    list(node_id = 2L, similarity = 0.80)
  )
  df <- client$results_to_dataframe(mock_results)
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 2L)
  expect_true("node_id" %in% names(df))
  expect_true("similarity" %in% names(df))
})

test_that("nodes_to_dataframe errors when not connected", {
  client <- AstraeaClient$new()
  expect_error(client$nodes_to_dataframe(c(1L, 2L)), "Not connected")
})

# ============================================================================
# New commands (integration tests)
# ============================================================================

test_that("dfs, find_by_label, and find_edge_by_type work on a live server", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("RParity_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  a <- client$create_node(labels = c(tag), properties = list(name = "a"))
  b <- client$create_node(labels = c(tag), properties = list(name = "b"))
  client$create_edge(a, b, edge_type = "RPARITY_LINK")
  withr::defer(tryCatch(client$delete_by_label(tag), error = function(e) NULL))

  visited <- client$dfs(a, max_depth = 2L)
  expect_type(visited, "list")

  found <- client$find_by_label(tag)
  expect_true(length(found) >= 2L)

  edges <- client$find_edge_by_type("RPARITY_LINK")
  expect_type(edges, "list")
})

test_that("graph_stats and algorithms return structured results", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  stats <- client$graph_stats()
  expect_type(stats, "list")
  expect_true("total_nodes" %in% names(stats))

  pr <- client$run_pagerank(max_iterations = 20L)
  expect_type(pr, "list")

  cc <- client$run_connected_components()
  expect_true("count" %in% names(cc))
})

test_that("delete_by_label removes every tagged node", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("RDelete_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  client$create_node(labels = c(tag), properties = list(x = 1))
  client$create_node(labels = c(tag), properties = list(x = 2))

  n <- client$delete_by_label(tag)
  expect_true(n >= 2L)
  expect_equal(length(client$find_by_label(tag)), 0L)
})

# ============================================================================
# Utility Functions (integration tests)
# ============================================================================

test_that("nodes_to_dataframe returns expected structure", {
  skip_if_no_server()

  client <- AstraeaClient$new()
  withr::defer(client$disconnect())
  client$connect()

  tag <- paste0("test_", format(Sys.time(), "%Y%m%d%H%M%OS6"))
  n1 <- client$create_node(
    labels = c("TestUtil"),
    properties = list(name = paste0(tag, "_u1"), val = 10)
  )
  n2 <- client$create_node(
    labels = c("TestUtil"),
    properties = list(name = paste0(tag, "_u2"), val = 20)
  )
  withr::defer({
    tryCatch(client$delete_node(n1), error = function(e) NULL)
    tryCatch(client$delete_node(n2), error = function(e) NULL)
  })

  df <- client$nodes_to_dataframe(c(n1, n2))
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 2L)
  expect_true("id" %in% names(df))
  expect_true("labels" %in% names(df))
})
