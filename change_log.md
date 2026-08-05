**1.2.2**

- fix: columns whose names differ only in letter case are now matched by name instead of
  by their position in the report file. A billing period that lists a case-colliding tag
  pair (e.g. `resourceTags/user:Team` and `resourceTags/user:team`) in a different order
  than another period is loaded correctly instead of aborting, with each tag's data under
  its own column. This also removes a false abort that needed no reordering at all: a
  report that starts carrying a second case variant of a tag now simply gets an additional
  column. Column names already recorded in the configuration state are reused unchanged,
  so existing output tables keep the columns they have.
- Which case variant owns which output column is now recorded in the state file under
  `report_column_slots`. The key is additive: state written by earlier versions is read as
  before, and the mapping is recovered from the stored header for those configurations.
- The 1.1.6 safety guard is superseded and removed. With name-based matching there is no
  positional swap left to protect against, so the abort — and its "contact Keboola
  support so the affected report can be reprocessed" instruction, which pointed at no
  actual procedure — is gone.
- fix: parse plain `YYYY-MM-DD` values of `min_date_since` / `max_date` directly rather
  than through dateparser, which emitted a Python `DeprecationWarning` about ambiguous
  dates on every single run and read like a component failure in the job log. Relative
  values such as `5 days ago` or `yesterday` still go through dateparser.

**1.1.6**

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
