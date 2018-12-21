#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from worldpop import generate_dataset_and_showcase, get_countriesdata

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-worldpop'


def main():
    """Generate dataset and create it in HDX"""

    json_url = Configuration.read()['json_url']
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        countriesdata = get_countriesdata(json_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in sorted(countriesdata, key=lambda x: x['Location']):
            dataset, showcase = generate_dataset_and_showcase(downloader, countrydata)
            dataset.update_from_yaml()
            dataset.create_in_hdx(hxl_update=False)
            showcase.create_in_hdx()
            showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
