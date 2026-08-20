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

The version is bumped to 0.2.1 for this resubmission, so that the submitted
tarball is distinguishable from the 0.2.0 that was reviewed. NEWS.md records
the changes above under a 0.2.1 heading.

## R CMD check results

0 errors | 0 warnings | 3 notes

Checked with `R CMD check --as-cran --run-donttest`, and additionally against a
live AstraeaDB server on the default port so that every `\donttest{}` example
actually executed rather than being silently skipped. All examples and all 110
tests pass under both conditions.

* checking CRAN incoming feasibility ... NOTE
  Maintainer: 'James Harris <jimeharrisjr@gmail.com>'
  New submission

  The standard note for a first submission. Five words in the Description are
  flagged as possibly misspelled. All five are correct:

  * "GQL" is Graph Query Language, the ISO/IEC 39075:2024 standard.
  * "Louvain" is the community-detection algorithm, named for the university.
  * "PageRank" is the ranking algorithm.
  * "betweenness" is the standard graph-theory term (betweenness centrality).
  * "lookups" is the plural of "lookup".

  Three of these ("GQL", "Louvain", "PageRank") are newly flagged in this
  submission, and only because of the requested change above: they previously
  sat inside single quotes, which the spell checker skips, and removing the
  quotes exposed them. They were already spelled this way.

### win-builder

Both win-builder runs of this tarball returned **1 NOTE** and nothing else, the
note being the incoming-feasibility one above:

* R 4.6.1 (2026-06-24 ucrt): 1 NOTE, `checking examples ... OK`,
  `checking tests ... [45s] OK`.
* R-devel (2026-08-17 r90424 ucrt): 1 NOTE, `checking examples ... OK`,
  `checking tests ... [46s] OK`.

Both runs were repeated on 2026-08-19 against the current R-devel snapshot and
returned the same single note, with no warnings and no errors.

This is the first check in which the examples actually ran on Windows, since
they were `\dontrun{}` previously. With no AstraeaDB server present the
`astraea_server_available()` guard returns FALSE and each `\donttest{}` example
is a no-op, which is the behaviour intended.

The two notes below appear only on the local macOS check host and do not reflect
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

* local: macOS 15, R 4.4.1, checked both without a server (each `\donttest{}`
  example a no-op, integration tests skipped) and against a live AstraeaDB
  server, where the full suite runs: 111 tests pass, 0 fail, 1 skipped (the
  optional Arrow Flight suite, as 'arrow' is not installed on that host).
* win-builder: R 4.6.1 (2026-06-24 ucrt) — 1 NOTE.
* win-builder: R-devel (2026-08-17 r90424 ucrt) — 1 NOTE.

The single note is the incoming-feasibility one discussed above and is
identical on both Windows environments.
