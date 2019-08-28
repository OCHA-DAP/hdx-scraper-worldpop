#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from worldpop import generate_dataset_and_showcase, get_url_iso3s, get_indicatorsdata

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-worldpop'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    indicators_to_process = configuration['indicators']
    json_url = configuration['json_url']
    with Download() as downloader:
        indicators = get_indicatorsdata(json_url, downloader)
        for indicator in indicators:
            if indicator not in indicators_to_process:
                continue
            base_url, iso3s = get_url_iso3s(json_url, downloader, indicator)

            logger.info('Number of datasets to upload: %d' % len(iso3s))
            for iso3 in iso3s:
                dataset, showcase = generate_dataset_and_showcase(downloader, base_url, indicators[indicator], iso3)
                if dataset is not None:
                    dataset.update_from_yaml()
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
