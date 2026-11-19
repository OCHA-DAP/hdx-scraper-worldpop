# 0002: Dataset text from the Google Sheet, licence and citation from the API

## Status

Accepted — 2026-11-19

## Context

Dataset text used to come from hub's per-record fields (`title`, `desc`,
`category`, `citation`). v3 has no descriptive text, only file URLs, citation
and licence. The "WorldPop Dataset Metadata" Google Sheet already holds titles,
descriptions, tags, caveats and resource text per alias (`pop`,
`age_structures`). A first attempt (the `gsheet` branch, merged then reverted)
hardcoded values in the sheet that vary by country or release, for example
"Zimbabwe" in the age description.

## Decision

- Descriptive text comes from the sheet. Fields that vary come from the API data
  through `{placeholders}`: `countryname`, `startyear`, `endyear`, `release`,
  `version`, `resolutions`, `resolution_details` and `example_file`
  (`DatasetGenerator.get_description_values`).
- Licence comes from the API and is mapped to an HDX `license_id` in
  `project_configuration.yaml` (`licences`). An unrecognised licence raises an
  error.
- Citation comes from the API and is appended to the sheet caveats unless
  already present. An empty citation raises an error.
- Placeholders are filled strictly: any `{name}` left in the text raises a
  `ValueError`, so a sheet typo fails the run instead of publishing broken text.
- The per-alias resolutions are taken from the API, not the sheet's
  Indicators tab, which is no longer read.

## Consequences

- Editing the sheet changes published text without a code change, and a bad
  edit fails the run.
- Literal braces cannot be used in sheet text.
- A new licence needs a `licences` entry before it can be published.
- The sheet must keep the field names the code reads (`Dataset Description`,
  `Caveats`, `Resource Name`, ...).
