#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
WORLDPOP:
------------

Reads WorldPop JSON.

'''

import logging

import sys
from datetime import datetime
from os.path import join, expanduser

from hdx.data.dataset import Dataset
from iso8601 import iso8601
from slugify import slugify
import requests

logger = logging.getLogger(__name__)


def load_worldpop_key(path: str) -> str:
    """
    Load WorldPop key

    Args:
        path (str): Path to WorldPop key

    Returns:
        str: WorldPop authorisation

    """
    with open(path, 'rt') as f:
        auth = f.read().replace('\n', '')
    if not auth:
        raise (ValueError('WorldPop key is empty!'))
    logger.info('Loaded WorldPop key from: %s' % path)
    return auth


def get_dataset_date(datestr):
    date = iso8601.parse_date(datestr)
    return date.date().strftime('%m/%d/%Y')


def generate_datasets(configuration, today, iso=None):
    '''Parse json of the form:
    {
      "Location": "Zimbabwe",
      "Dataset Title": "WorldPop Zimbabwe Population dataset",
      "Source": "WorldPop, University of Southampton, UK",
      "Description": "These datasets provide estimates of population counts for each 100 x 100m grid cell in the country for various years. Please refer to the metadata file and WorldPop website (www.worldpop.org) for full information.",
      "Dataset contains sub-national data": "true",
      "License": "Other",
      "Define License": "http://www.worldpop.org.uk/data/licence.txt",
      "Organisation": "WorldPop, University of Southampton, UK; www.worldpop.org",
      "Visibility": "Public",
      "id_no": "243",
      "URL_direct": "http://www.worldpop.org.uk/data/hdx/?dataset=ZWE-POP",
      "URL_summaryPage": "http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population",
      "URL_datasetDetailsPage": "http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP.txt",
      "URL_image": "http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP_500.JPG",
      "productionDate": "2013-01-01T00:00:00+00:00",
      "datasetDate": "2015",
      "lastModifiedDate": "2016-10-17T12:54:54+01:00",
      "fileFormat": "zipped geotiff",
      "location": "ZWE",
      "updateFrequency": "Annual",
      "maintainerName": "WorldPop",
      "maintainerEmail": "worldpop@geodata.soton.ac.uk",
      "authorName": "WorldPop",
      "authorEmail": "worldpop@geodata.soton.ac.uk",
      "tags": [
        "Population Statistics",
        "WorldPop",
        "University of Southampton"
      ]
    },
    '''
    auth = load_worldpop_key(join(expanduser("~"), '.worldpopkey'))
    r = requests.get(configuration['json_url'], headers = {'Authorization': auth})
    r.raise_for_status()
    worldpopdata = r.json()['worldPopData']
    logger.info('Number of datasets to upload: %d' % len(worldpopdata))
    for countrydata in worldpopdata:
        title = countrydata['Dataset Title'].replace(' dataset', '')
        logger.info('Creating dataset: %s' % title)
        licence_id = countrydata['License'].lower()
        licence = None
        if licence_id == 'other':
            licence_id = 'hdx-other'
            licence_url = countrydata['Define License']
            r = requests.get(licence_url)
            r.raise_for_status()
            licence = r.text
        slugified_name = slugify(title).lower()
        url_summary = countrydata['URL_summaryPage']
        description = 'Go to [WorldPop Dataset Summary Page](%s) for more information' % url_summary
        dataset = Dataset(configuration, {
            'name': slugified_name,
            'title': title,
            'notes': countrydata['Description'],
            'methodology': 'Other',
            'methodology_other': description,
            'dataset_source': countrydata['Source'],
            'subnational': countrydata['Dataset contains sub-national data'] == True,
            'license_id': licence_id,
            'private': countrydata['Visibility'] != 'Public',
            'url': url_summary,
            'author': countrydata['authorName'],
            'author_email': countrydata['authorEmail'],
            'maintainer': countrydata['maintainerName'],
            'maintainer_email': countrydata['maintainerEmail'],
        })
        dataset.set_dataset_date(countrydata['datasetDate'])
        dataset.set_expected_update_frequency(countrydata['updateFrequency'])
        dataset.add_country_location(countrydata['location'])
        dataset.add_tags(countrydata['tags'])
        if licence:
            dataset.update({'license_other': licence})

        resource = {
            'name': title,
            'format': countrydata['fileFormat'],
            'url': countrydata['URL_direct'],
            'description': description,
            'url_type': 'api',
            'resource_type': 'api'
        }
        dataset.add_update_resource(resource)

        galleryitem = {
            'title': 'WorldPop Dataset Summary Page',
            'type': 'post',
            'description': '%s Summary Page' % title,
            'url': url_summary,
            'image_url': countrydata['URL_image']
        }
        dataset.add_update_galleryitem(galleryitem)
        dataset.update_from_yaml()
        dataset.create_in_hdx()
