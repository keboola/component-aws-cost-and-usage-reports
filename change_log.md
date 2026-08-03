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
