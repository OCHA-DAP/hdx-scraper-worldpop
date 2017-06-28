#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldpop import generate_dataset, get_countriesdata

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    json_url = Configuration.read()['json_url']
    downloader = Download(basicauthfile=join(expanduser("~"), '.worldpopkey'))
    countriesdata = get_countriesdata(json_url, downloader)
    logger.info('Number of datasets to upload: %d' % len(countriesdata))
    for countrydata in countriesdata:
        dataset = generate_dataset(downloader, countrydata)
        if dataset is not None:
            dataset.update_from_yaml()
            dataset.create_in_hdx()

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
