**1.2.2**

- fix: remove the column-order safety guard released in 1.2.1. It aborted the run when a
  case-differing tag pair *looked* reordered across billing periods, but it inferred that
  from a letter-case mismatch between a file's deduplicated column name and the name held
  in state — and the deduplicating suffix is assigned by physical column position, so the
  deduplicated name is not a stable identifier for a column. The guard therefore fired
  with nothing reordered at all: a state seeded when only one case variant existed, a
  period carrying only one of the two variants, or simply the order the report files are
  listed in were each enough to trigger it. Since the state file is only written on
  success, an affected configuration failed identically on every subsequent run, and the
  message asked the user to contact support for a reprocessing procedure that does not
  exist. Case-differing tag columns are still kept distinct — that fix (1.2.1) stays.
  Trade-off, stated plainly: the per-file header override is applied positionally, so for a
  report that *genuinely* lists a case-differing tag pair in a different order in different
  billing periods, those two tags' values are exchanged for the reordered period. Releases
  before 1.2.1 handled that input worse still, collapsing the pair and losing one tag's
  values altogether. If you rely on such a pair, check the two columns after a backfill.
- fix: parse plain `YYYY-MM-DD` values of `min_date_since` / `max_date` directly instead of
  through dateparser, which emitted a Python `DeprecationWarning` about ambiguous dates on
  every single run, since `min_date_since` defaults to a plain date. The warning names a
  Python version and reads like a component failure in the job log. Relative values such as
  `5 days ago` or `yesterday` still go through dateparser.
- The project version was left at 1.1.6 while 1.2.0 and 1.2.1 were released from tags; it
  is realigned here.

**1.1.6** (never released on its own; shipped inside 1.2.1)

- safety: fail loudly instead of silently swapping data if a report lists a
  case-differing tag pair (e.g. `resourceTags/user:Team` and `resourceTags/user:team`)
  in a different physical column order across billing periods. The header override is
  applied positionally, so an inconsistent order would put one tag's data under the
  other tag's column; the run now aborts with a clear message before writing anything.
  This is a defensive guard only — it has no effect on reports whose column order is
  consistent (the normal case), so existing configurations are unaffected.

**1.1.5**

- fix: keep AWS CUR tag columns that differ only by letter case as separate columns
  with their own data (SUPPORT-17124). Report files are now read with explicit,
  case-insensitively unique column names so DuckDB's case-insensitive header handling
  no longer collapses columns such as `resourceTags/user:Team` and
  `resourceTags/user:team`. Files that share an identical column layout are read with a
  single multi-file reader and only distinct layouts are combined with
  `UNION ALL BY NAME`, preserving the original low-memory streaming behaviour.
  Reads use `union_by_name` + `null_padding`, so a chunk with fewer columns than its
  manifest is NULL-padded rather than aborting the export (matches the previous
  tolerant behaviour).

**0.1.1**

- fix requirements
- add src folder to path for tests

**0.1.0**

- src folder structure
- remove dependency on handler lib - import the code directly to enable modifications until its released

**0.0.2**

- add dependency to base lib
- basic tests

**0.0.1**

- add utils scripts
- move kbc tests directly to pipelines file
- use uptodate base docker image
- add changelog
