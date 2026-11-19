# 0001: Use WorldPop API v3 for file links

## Status

Accepted — 2026-11-18

## Context

The pipeline read file links from WorldPop Hub (`hub.worldpop.org/rest/data/`),
which needs about 970 calls per run. WorldPop now has an official API
(`api.worldpop.org/v3`). One call, `GET /countries/{iso3}/files?years=all`,
returns every year's URLs for population and age/sex at 100m and 1km, plus
citation and licence. The file URLs match hub's (checked for AFG 2025).

## Decision

- Read `/countries` and then `/countries/{iso3}/files?years=all` per country,
  about 241 calls per run.
- Skip countries with `total_population: null` (ATF, HMD, IOT, SGS, UMI, VAT)
  and aliases whose years are all null.
- Use `XKX` for Kosovo as v3 does, so the `KOS` remap is removed and Kosovo
  resource names start `xkx_`.
- Keep hub only to find the id of each record for the showcase URL
  (`hub_summary_url`), because v3 has no summary page. The showcase image is the
  v3 profile PNG (`/profiles/place.png?iso3=...`).
- Use an optional API key (`WORLDPOP_API_KEY`). Without one the anonymous limits
  apply (1000 requests a day, 60 a minute).

## Consequences

- CCK and CXR exist on hub but return 404 in v3, so their HDX datasets stop
  updating.
- Showcase URLs still depend on hub, so a hub outage or a missing record falls
  back to the default URL with an error logged.
- The showcase image changes from hub's thumbnail to the v3 profile PNG.
- Resource format stays `Geotiff` for both aliases, as before, although the age
  files are zips. It is set in `project_configuration.yaml`.
