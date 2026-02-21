#!/usr/bin/env Rscript
#
# pcap_to_graph.R
#
# Continuously captures network traffic and writes it to AstraeaDB as a graph:
#   - IP addresses become nodes (label: "IPAddress")
#   - Flows become edges (type: TCP, UDP, ICMP, etc.)
#     aggregated by (src_ip, dst_ip, protocol, service_port) with properties
#     for service port, service name, flow count, total bytes, and temporal
#     window (valid_from / valid_to).
#
# Usage:
#   sudo Rscript pcap_to_graph.R [interface] [chunk_size] [host] [port]
#
# Examples:
#   sudo Rscript pcap_to_graph.R en0
#   sudo Rscript pcap_to_graph.R en0 50
#   sudo Rscript pcap_to_graph.R en0 50 127.0.0.1 7687
#
# Note: live capture typically requires root/sudo privileges.

library(Rtins)
library(AstraeaDB)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

IFACE      <- if (length(args) >= 1) args[1]                else "en0"
CHUNK_SIZE <- if (length(args) >= 2) as.integer(args[2])    else 50L
DB_HOST    <- if (length(args) >= 3) args[3]                else "127.0.0.1"
DB_PORT    <- if (length(args) >= 4) as.integer(args[4])    else 7687L
BPF_FILTER <- "ip"   # capture only IP traffic at the BPF level

# ---------------------------------------------------------------------------
# Connect to AstraeaDB
# ---------------------------------------------------------------------------

message("Connecting to AstraeaDB at ", DB_HOST, ":", DB_PORT, " ...")
client <- astraea_connect(host = DB_HOST, port = DB_PORT)
message("Connected.")

# Local cache of IP -> node ID so we don't create duplicate nodes.
ip_node_map <- new.env(hash = TRUE, parent = emptyenv())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Look up or create a node for an IP address.
#' Returns the AstraeaDB node ID.
get_or_create_ip_node <- function(ip) {
  if (exists(ip, envir = ip_node_map)) {
    return(get(ip, envir = ip_node_map))
  }
  node_id <- client$create_node(
    labels     = "IPAddress",
    properties = list(ip = ip)
  )
  assign(ip, node_id, envir = ip_node_map)
  node_id
}

#' Process a chunk of captured packets: aggregate into flows and write edges.
#' Returns the number of flows written.
process_chunk <- function(pkts) {
  # Keep only rows where layer 2 is IP
  ip_rows <- pkts[pkts$layer_2_id == "IP", ]
  if (nrow(ip_rows) == 0L) return(0L)

  # Collect unique IPs and ensure nodes exist
  all_ips <- unique(c(ip_rows$layer_2_src, ip_rows$layer_2_dst))
  for (ip in all_ips) {
    get_or_create_ip_node(ip)
  }

  # Compute timestamps (milliseconds since epoch)
  ts_ms <- as.numeric(ip_rows$tv_sec) * 1000 +
    as.numeric(ip_rows$tv_usec) %/% 1000

  # Determine service port for each row
  src_port <- suppressWarnings(as.integer(ip_rows$layer_3_src))
  dst_port <- suppressWarnings(as.integer(ip_rows$layer_3_dst))
  svc_port <- classify_service_port(src_port, dst_port)

  # Protocol: use transport layer, fall back to "IP"
  protocol <- ip_rows$layer_3_id
  protocol <- ifelse(is.na(protocol) | protocol == "", "IP", protocol)

  # Build flow key for aggregation
  flow_key <- paste(
    ip_rows$layer_2_src,
    ip_rows$layer_2_dst,
    protocol,
    ifelse(is.na(svc_port), "NA", svc_port),
    sep = "|"
  )

  # Aggregate using tapply (do NOT wrap in as.integer/as.numeric —
  # that strips dimnames and breaks name-based indexing below).
  unique_keys <- unique(flow_key)
  n_flows <- length(unique_keys)

  flow_count  <- tapply(rep(1L, length(flow_key)), flow_key, sum)
  total_bytes <- tapply(ip_rows$layer_1_size, flow_key, sum, na.rm = TRUE)
  first_seen  <- tapply(ts_ms, flow_key, min, na.rm = TRUE)
  last_seen   <- tapply(ts_ms, flow_key, max, na.rm = TRUE)

  # Build edge list
  edges <- vector("list", n_flows)
  for (i in seq_along(unique_keys)) {
    key <- unique_keys[i]
    parts <- strsplit(key, "\\|", fixed = FALSE)[[1]]
    src_ip   <- parts[1]
    dst_ip   <- parts[2]
    etype    <- parts[3]
    sp       <- suppressWarnings(as.integer(parts[4]))

    src_id <- get(src_ip, envir = ip_node_map)
    dst_id <- get(dst_ip, envir = ip_node_map)

    svc_name <- if (!is.na(sp)) port_service_name(sp) else NA_character_

    fc <- flow_count[[key]]
    tb <- total_bytes[[key]]
    vf <- first_seen[[key]]
    vt <- last_seen[[key]]

    props <- list(
      service_port = if (is.na(sp)) NULL else as.integer(sp),
      service_name = if (is.na(svc_name)) NULL else svc_name,
      flow_count   = if (is.na(fc)) NULL else as.integer(fc),
      total_bytes  = if (is.na(tb)) NULL else as.numeric(tb)
    )

    edges[[i]] <- list(
      source     = src_id,
      target     = dst_id,
      edge_type  = etype,
      properties = props,
      valid_from = if (is.na(vf) || !is.finite(vf)) NULL else as.numeric(vf),
      valid_to   = if (is.na(vt) || !is.finite(vt)) NULL else as.numeric(vt)
    )
  }

  client$create_edges(edges)
  n_flows
}

# ---------------------------------------------------------------------------
# Main capture loop
# ---------------------------------------------------------------------------

message(sprintf(
  "Capturing on '%s' with BPF filter '%s', %d packets per chunk.",
  IFACE, BPF_FILTER, CHUNK_SIZE
))
message("Press Ctrl-C to stop.\n")

total_packets <- 0L
total_flows   <- 0L
total_nodes   <- 0L

tryCatch({
  repeat {
    pkts <- sniff_pcap(
      iface  = IFACE,
      filter = BPF_FILTER,
      num    = CHUNK_SIZE,
      layers = 3L
    )

    n_ip <- sum(pkts$layer_2_id == "IP", na.rm = TRUE)
    chunk_flows <- 0L
    if (n_ip > 0L) {
      chunk_flows <- process_chunk(pkts)
    }

    total_nodes   <- length(ls(ip_node_map))
    total_flows   <- total_flows + chunk_flows
    total_packets <- total_packets + nrow(pkts)

    message(sprintf(
      "[%s] chunk: %d pkts (%d IP, %d flows) | totals: %d packets, %d flows, %d unique IPs",
      format(Sys.time(), "%H:%M:%S"),
      nrow(pkts), n_ip, chunk_flows,
      total_packets, total_flows, total_nodes
    ))
  }
},
interrupt = function(e) {
  message("\nCapture stopped by user.")
},
error = function(e) {
  message("\nError: ", conditionMessage(e))
},
finally = {
  message(sprintf(
    "Final totals: %d packets captured, %d flows written, %d unique IP nodes.",
    total_packets, total_flows, total_nodes
  ))
  message("Disconnecting from AstraeaDB ...")
  client$disconnect()
  message("Done.")
})
