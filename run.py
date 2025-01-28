#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join
from hdx.facades.infer_arguments import facade
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import wheretostart_tempdir_batch, progress_storing_folder
from hdx.utilities.retriever import Retrieve

from worldpop import WorldPop

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-worldpop"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
    Returns:
        None
    """

    configuration = Configuration.read()
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            worldpop = WorldPop(retriever, configuration)
            indicators_metadata = worldpop.get_indicators_metadata()
            countriesdata, countries = worldpop.get_countriesdata()
            logger.info(f"Number of countries to upload: {len(countries)}")

            for _, country in progress_storing_folder(info, countries, "iso3"):
                countryiso = country["iso3"]
                datasets, showcases = worldpop.generate_datasets_and_showcases(
                    countryiso, indicators_metadata, countriesdata[countryiso]
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
        hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
