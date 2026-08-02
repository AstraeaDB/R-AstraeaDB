## Submission

This is a new submission of 'AstraeaDB', an R client for the AstraeaDB graph
database.

## R CMD check results

0 errors | 0 warnings | 1 note

* checking CRAN incoming feasibility ... NOTE
  Maintainer: 'James Harris <jimeharrisjr@gmail.com>'
  New submission
  Possibly misspelled words in DESCRIPTION:
    betweenness (12:32)
    lookups (9:48)

  This is the standard note for a first submission. The two flagged words
  are spelled correctly: "betweenness" is the standard graph-theory term
  (betweenness centrality) and "lookups" is the plural of "lookup". Neither
  is in the aspell dictionary used by the incoming check.

Local `R CMD check --as-cran` on macOS additionally reports two notes that are
specific to the check environment and do not reflect package problems:

* "checking for future file timestamps ... NOTE: unable to verify current
  time" — the local check host had no network access to the time server.
* "checking HTML version of manual ... NOTE" — the local copy of HTML Tidy
  predates the HTML5 `<main>` element that R (>= 4.4) emits in help pages;
  CRAN's toolchain does not report this.

## Notes for the reviewer

* AstraeaDB is a client for a separate database server. Every example, test,
  and vignette that requires a running server is guarded so the package checks
  cleanly without one:
  * Examples that connect to a server are wrapped in `\dontrun{}`; examples for
    pure helper functions run normally.
  * Integration tests are skipped when no server is reachable
    (`skip_if_no_server()`), and the optional Arrow Flight tests are skipped
    when the suggested 'arrow' package is not installed.
  * All vignettes set `eval = FALSE`, so they build without a server (their
    code is shown for documentation but not executed).

## Test environments

* local: macOS, R 4.4.1
