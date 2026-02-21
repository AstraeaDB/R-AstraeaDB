#' @title Port Classification Utilities
#' @name ports
#' @description Functions for classifying network service ports and looking up
#'   well-known port names. Used by the PCAP ingestion script to aggregate
#'   packets into flows keyed by service port.
NULL

# ---- Internal: well-known ports lookup ----

#' Named character vector mapping common port numbers to service names.
#' Covers well-known ports (0-1023) and key registered/common ports.
#' @noRd
.well_known_ports <- c(

  # Well-known ports (0-1023)
  "7"    = "Echo",
  "20"   = "FTP-data",
  "21"   = "FTP",
  "22"   = "SSH",
  "23"   = "Telnet",
  "25"   = "SMTP",
  "43"   = "WHOIS",
  "53"   = "DNS",
  "67"   = "DHCP-server",
  "68"   = "DHCP-client",
  "69"   = "TFTP",
  "80"   = "HTTP",
  "88"   = "Kerberos",
  "110"  = "POP3",
  "111"  = "RPCbind",
  "119"  = "NNTP",
  "123"  = "NTP",
  "135"  = "MS-RPC",
  "137"  = "NetBIOS-NS",
  "138"  = "NetBIOS-DGM",
  "139"  = "NetBIOS-SSN",
  "143"  = "IMAP",
  "161"  = "SNMP",
  "162"  = "SNMP-trap",
  "179"  = "BGP",
  "194"  = "IRC",
  "389"  = "LDAP",
  "443"  = "HTTPS",
  "445"  = "SMB",
  "464"  = "Kerberos-pw",
  "465"  = "SMTPS",
  "500"  = "ISAKMP",
  "514"  = "Syslog",
  "515"  = "LPD",
  "520"  = "RIP",
  "530"  = "RPC",
  "543"  = "Klogin",
  "544"  = "Kshell",
  "546"  = "DHCPv6-client",
  "547"  = "DHCPv6-server",
  "554"  = "RTSP",
  "587"  = "Submission",
  "593"  = "MS-DCOM",
  "631"  = "IPP",
  "636"  = "LDAPS",
  "691"  = "MS-Exchange",
  "749"  = "Kerberos-admin",
  "853"  = "DNS-over-TLS",
  "873"  = "rsync",
  "902"  = "VMware",
  "993"  = "IMAPS",

  "995"  = "POP3S",

  # Registered / common ports (1024+)
  "1080" = "SOCKS",
  "1194" = "OpenVPN",
  "1433" = "MS-SQL",

  "1434" = "MS-SQL-Browser",
  "1521" = "Oracle",
  "1723" = "PPTP",
  "1883" = "MQTT",
  "2049" = "NFS",
  "2181" = "ZooKeeper",
  "2375" = "Docker",
  "2376" = "Docker-TLS",
  "3000" = "Grafana",
  "3306" = "MySQL",
  "3389" = "RDP",
  "3478" = "STUN",
  "4443" = "Pharos",
  "4500" = "IPsec-NAT",
  "5000" = "UPnP",

  "5060" = "SIP",
  "5061" = "SIP-TLS",
  "5222" = "XMPP",
  "5228" = "Google-Play",
  "5353" = "mDNS",
  "5432" = "PostgreSQL",
  "5671" = "AMQP-TLS",
  "5672" = "AMQP",
  "5900" = "VNC",
  "5984" = "CouchDB",
  "6379" = "Redis",
  "6443" = "Kubernetes",
  "6514" = "Syslog-TLS",
  "6660" = "IRC-alt",
  "6667" = "IRC",
  "6697" = "IRC-TLS",
  "7001" = "WebLogic",
  "7474" = "Neo4j",
  "7687" = "Bolt",
  "8000" = "HTTP-alt",
  "8080" = "HTTP-proxy",
  "8081" = "HTTP-alt2",
  "8443" = "HTTPS-alt",
  "8500" = "Consul",
  "8883" = "MQTT-TLS",
  "8888" = "HTTP-alt3",
  "9000" = "SonarQube",
  "9042" = "Cassandra",
  "9090" = "Prometheus",
  "9092" = "Kafka",
  "9200" = "Elasticsearch",
  "9300" = "ES-transport",
  "9418" = "Git",
  "10000" = "Webmin",
  "11211" = "Memcached",
  "15672" = "RabbitMQ-mgmt",
  "27017" = "MongoDB",
  "27018" = "MongoDB-shard",
  "27019" = "MongoDB-config",
  "50000" = "SAP"
)


#' Classify the service port from a source/destination port pair
#'
#' Given vectors of source and destination ports, determines which port
#' represents the "service" (server) side of the connection using a three-tier
#' heuristic:
#' \enumerate{
#'   \item If exactly one port is in the privileged range (< 1024), it is the
#'         service port.
#'   \item If exactly one port appears in the well-known ports lookup table,
#'         it wins.
#'   \item Otherwise the lower port number is chosen (statistically more likely
#'         to be the service).
#' }
#'
#' @param src_port Integer vector of source port numbers.
#' @param dst_port Integer vector of destination port numbers.
#' @return Integer vector of the same length containing the service port for
#'   each pair. Returns \code{NA_integer_} where either input is \code{NA}.
#' @export
#' @examples
#' classify_service_port(52000, 443)
#' classify_service_port(c(52000, 3306), c(80, 52000))
classify_service_port <- function(src_port, dst_port) {
  src_port <- as.integer(src_port)
  dst_port <- as.integer(dst_port)

  n <- max(length(src_port), length(dst_port))
  src_port <- rep_len(src_port, n)
  dst_port <- rep_len(dst_port, n)

  result <- rep(NA_integer_, n)

  # Identify non-NA pairs
  valid <- !is.na(src_port) & !is.na(dst_port)

  s <- src_port[valid]
  d <- dst_port[valid]

  # Tier 1: privileged range (< 1024)
  s_priv <- s < 1024L
  d_priv <- d < 1024L
  tier1 <- s_priv != d_priv  # exactly one is privileged

  # Tier 2: well-known ports lookup
  port_names <- names(.well_known_ports)
  s_known <- as.character(s) %in% port_names
  d_known <- as.character(d) %in% port_names
  tier2 <- !tier1 & (s_known != d_known)  # exactly one is known

  # Tier 3: lower port wins (fallback)
  tier3 <- !tier1 & !tier2

  chosen <- integer(sum(valid))
  chosen[tier1] <- ifelse(s_priv[tier1], s[tier1], d[tier1])
  chosen[tier2] <- ifelse(s_known[tier2], s[tier2], d[tier2])
  chosen[tier3] <- pmin(s[tier3], d[tier3])

  result[valid] <- chosen
  result
}


#' Look up the service name for a port number
#'
#' Returns the human-readable service name for a given port number based on
#' the package's built-in well-known ports table, or \code{NA_character_} if
#' the port is not recognized.
#'
#' @param port Integer vector of port numbers.
#' @return Character vector of service names, with \code{NA_character_} for
#'   unrecognized ports.
#' @export
#' @examples
#' port_service_name(443)
#' port_service_name(c(80, 3306, 99999))
port_service_name <- function(port) {
  port <- as.character(as.integer(port))
  result <- unname(.well_known_ports[port])
  result[is.na(result)] <- NA_character_
  result
}
