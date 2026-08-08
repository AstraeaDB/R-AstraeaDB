## Resubmission

This is a resubmission. Thank you for the review. All four points are addressed
below.

* **"Please omit the redundant 'an R' at the start of your description."**
  The Description field now opens "Provides a client for 'AstraeaDB', ...".

* **"Please only write package names, software names and API names in single
  quotes ... omit them around acronyms, names, etc., e.g. BFS."**
  Quoting in Title and Description is now limited to software and package
  names: 'AstraeaDB', 'Apache Arrow Flight', and 'arrow'. The quotes are gone
  from BFS, DFS, PageRank, Louvain, GQL, GraphRAG, JSON, and TCP. While making
  that change I also spelled out the acronyms that remain, so the Description
  now reads "breadth-first search, depth-first search", "Graph Query Language
  (GQL)", and "graph-based retrieval-augmented generation".

* **"Please replace \dontrun with \donttest ... unwrap the examples if they are
  executable in < 5 sec."**
  There is no `\dontrun{}` left in the package. Of the 60 blocks:
  * 2 are now unwrapped and run unconditionally, because they need no server
    and finish in milliseconds: the `AstraeaClient$new()` constructor example
    and the `astraea_server_available()` example.
  * 58 are `\donttest{}` and are guarded by `if (astraea_server_available())`,
    using an exported helper the package already provided. AstraeaDB is a
    client for a separate database server, so with no server running the guard
    returns `FALSE` in about two seconds and the example is a no-op. With a
    server running the examples genuinely execute, which we verified (see
    below).

* **"Please wrap examples that need packages in 'Suggests' in
  if(requireNamespace("pkgname")){}."**
  Every example touching the optional Arrow Flight transport is now guarded
  with `if (requireNamespace("arrow", quietly = TRUE) && ...)`. The 'arrow'
  package remains in Suggests only.

Three further changes came out of actually running the examples, which
`\dontrun{}` had been hiding:

* Examples no longer reference hard-coded node and edge IDs such as `1L`. Each
  now creates the nodes it needs and uses the returned IDs, so it is
  self-contained against any server rather than assuming a particular database
  state.
* Examples that attach an embedding now size the vector from
  `client$ping()$vector_dim` rather than using a fixed three-element vector. A
  store pins its embedding width on first insert, so a literal length only
  worked against one server configuration.
* `UnifiedClient$query_df()` failed with "arguments imply differing number of
  rows" when a matched node lacked one of the requested properties, and failed
  again on rows carrying different property sets. It now maps an absent
  property to `NA` and takes the union of columns across rows. One integration
  test was widened to cover this.

No version bump: the package has not been on CRAN, so this supersedes the
previous submission of 0.2.0.

## R CMD check results

0 errors | 0 warnings | 3 notes

Checked with `R CMD check --as-cran --run-donttest`, and additionally against a
live AstraeaDB server on the default port so that every `\donttest{}` example
actually executed rather than being silently skipped. All examples and all 110
tests pass under both conditions.

* checking CRAN incoming feasibility ... NOTE
  Maintainer: 'James Harris <jimeharrisjr@gmail.com>'
  New submission

  The standard note for a first submission. Earlier submissions also flagged
  "betweenness" and "lookups" as possibly misspelled. Both are correct:
  "betweenness" is the standard graph-theory term (betweenness centrality) and
  "lookups" is the plural of "lookup".

Two further notes are specific to the local check host and do not reflect
package problems:

* "checking for future file timestamps ... NOTE: unable to verify current
  time" — the check host had no network access to the time server.
* "checking HTML version of manual ... NOTE" — the local copy of HTML Tidy
  predates the HTML5 `<main>` element that R (>= 4.4) emits in help pages.

## Notes for the reviewer

AstraeaDB is a client for a separate database server, so nothing in the package
can reach a server during a CRAN check. Each layer is guarded accordingly:

* Examples requiring a server are `\donttest{}` plus an
  `astraea_server_available()` guard, as described above.
* Integration tests are skipped when no server is reachable
  (`skip_if_no_server()`), and the optional Arrow Flight tests are skipped when
  the suggested 'arrow' package is not installed.
* All vignettes set `eval = FALSE`, so they build without a server. Their code
  is shown for documentation but not executed.

## Test environments

* local: macOS, R 4.4.1 (with and without a live AstraeaDB server)
