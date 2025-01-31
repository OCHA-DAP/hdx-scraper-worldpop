#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.worldpop._version import __version__
from hdx.scraper.worldpop.pipeline import Pipeline
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

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

    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access(
        "3f077dff-1d05-484d-a7c2-4cb620f22689", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to WorldPop organisation!"
        )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            worldpop = Pipeline(retriever, configuration)
            worldpop.get_indicators_metadata()
            _, countries = worldpop.get_countriesdata()
            logger.info(f"Number of countries to upload: {len(countries)}")

            for _, country in progress_storing_folder(info, countries, "iso3"):
                countryiso3 = country["iso3"]
                datasets, showcases = worldpop.generate_datasets_and_showcases(
                    countryiso3
                )
                for i, dataset in enumerate(datasets):
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: WorldPop",
                        batch=info["batch"],
                    )
                    showcase = showcases[i]
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)

    logger.info("HDX Scraper WorldPop pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="stage",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
