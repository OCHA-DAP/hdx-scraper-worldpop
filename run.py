#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldpop import generate_dataset_and_showcase, get_countriesdata

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    json_url = Configuration.read()['json_url']
    downloader = Download(basicauthfile=join(expanduser("~"), '.worldpopkey'))
    countriesdata = get_countriesdata(json_url, downloader)
    logger.info('Number of datasets to upload: %d' % len(countriesdata))
    for countrydata in countriesdata:
        dataset, showcase = generate_dataset_and_showcase(downloader, countrydata)
        dataset.update_from_yaml()
        dataset.create_in_hdx()
        showcase.create_in_hdx()
        showcase.add_dataset(dataset)

if __name__ == '__main__':
    facade(main, hdx_site='feature', project_config_yaml=join('config', 'project_configuration.yml'))
