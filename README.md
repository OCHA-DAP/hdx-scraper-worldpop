### WorldPop Pipeline
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-worldpop/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-worldpop/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-worldpop/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-worldpop?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script connects to the [WorldPop API v3](https://api.worldpop.org/v3/) and extracts population and age/sex file links country by country, creating datasets per country in HDX. Dataset text comes from the "WorldPop Dataset Metadata" Google Sheet. It makes around 250 reads from WorldPop and then 5000 read/writes (API calls) to HDX in a one hour period. It does not create temporary files as it puts urls into HDX. It runs every year.


### Usage

    python -m hdx.scraper.worldpop

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-worldpop** as specified in the parameter *user_agent_lookup*.

Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, BASIC_AUTH, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY

The script also needs:

- GSHEET_AUTH: Google service account JSON with read access to the metadata sheet (or pass `--gsheet-auth`)
- WORLDPOP_API_KEY: WorldPop API v3 key, sent as the `X-API-Key` header (or pass `--worldpop-api-key`). Without it the script runs at the anonymous limits (1000 requests a day, 60 a minute). Keys are requested at https://api.worldpop.org/v3/register
