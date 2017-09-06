#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.utilities.location import Location
from slugify import slugify

logger = logging.getLogger(__name__)


def get_countriesdata(json_url, downloader):
    response = downloader.download(json_url)
    worldpopjson = response.json()
    return worldpopjson['worldPopData']


def generate_dataset_and_showcase(downloader, countrydata):
    """Parse json of the form:
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
    """
    title = '%s - Population' % countrydata['Location']
    logger.info('Creating dataset: %s' % title)
    licence_id = countrydata['License'].lower()
    licence = None
    if licence_id == 'other':
        licence_id = 'hdx-other'
        licence_url = countrydata['Define License']
        response = downloader.download(licence_url)
        licence = response.text
    resource_name = countrydata['Dataset Title'].replace('dataset', '').strip()
    slugified_name = slugify(resource_name).lower()
    url_summary = countrydata['URL_summaryPage']
    description = 'Go to [WorldPop Dataset Summary Page](%s) for more information' % url_summary
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        'notes': countrydata['Description'],
        'methodology': 'Other',
        'methodology_other': description,
        'dataset_source': countrydata['Source'],
        'subnational': countrydata['Dataset contains sub-national data'] is True,
        'license_id': licence_id,
        'private': countrydata['Visibility'] != 'Public',
        'url': url_summary
    })
    dataset.set_maintainer('37023db4-a571-4f28-8d1f-15f0353586af')
    dataset.set_organization('3f077dff-1d05-484d-a7c2-4cb620f22689')
    dataset.set_dataset_date(countrydata['datasetDate'])
    dataset.set_expected_update_frequency(countrydata['updateFrequency'])
    dataset.add_country_location(countrydata['iso3'])
    tags = countrydata['tags']
    dataset.add_tags(tags)
    if licence:
        dataset.update({'license_other': licence})

    resource = {
        'name': resource_name,
        'format': countrydata['fileFormat'],
        'url': countrydata['URL_direct'],
        'description': description
    }
    dataset.add_update_resource(resource)

    location = countrydata['Location']
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'WorldPop %s Summary Page' % location,
        'notes': 'Click the image on the right to go to the WorldPop summary page for the %s dataset' % location,
        'url': url_summary,
        'image_url': countrydata['URL_image']
    })
    showcase.add_tags(tags)
    return dataset, showcase
