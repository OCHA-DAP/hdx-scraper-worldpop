#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join
from hdx.facades.simple import facade
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from worldpop import (
    generate_datasets_and_showcases,
    get_countriesdata,
    get_indicators_metadata,
)

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-worldpop"


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    indicators = configuration["indicators"]
    json_url = configuration["json_url"]
    with Download() as downloader:
        indicators_metadata = get_indicators_metadata(json_url, downloader, indicators)
        countriesdata, countries = get_countriesdata(json_url, downloader, indicators)
        logger.info(f"Number of countries to upload: {len(countries)}")

        for info, country in progress_storing_tempdir("WorldPop", countries, "iso3"):
            countryiso = country["iso3"]
            datasets, showcases = generate_datasets_and_showcases(
                downloader, countryiso, indicators_metadata, countriesdata[countryiso]
            )
            for dataset in datasets:
                dataset.update_from_yaml()
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script="HDX Scraper: WorldPop",
                    batch=info["batch"],
                )
                for showcase in showcases.get(dataset["name"], list()):
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
