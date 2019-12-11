#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging
import re

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)


def get_indicatorsdata(json_url, downloader):
    response = downloader.download(json_url)
    json = response.json()
    return {x['alias']: x for x in json['data']}


def get_url_iso3s(json_url, downloader, indicator):
    base_url = '%s%s' % (json_url, indicator)
    response = downloader.download(base_url)
    json = response.json()
    category = json['data'][-1]['alias']
    base_url = '%s/%s' % (base_url, category)
    response = downloader.download(base_url)
    json = response.json()
    return base_url, {x['iso3']: x for x in json['data']}


def generate_dataset_and_showcase(downloader, base_url, indicator, iso3):
    """Parse json of the form:
    {'id': '1482', 'title': 'The spatial distribution of population in 2000,
        Zimbabwe', 'desc': 'Estimated total number of people per grid-cell...',  'doi': '10.5258/SOTON/WP00645',
        'date': '2018-11-01', 'popyear': '2000', 'citation': 'WorldPop',
        'data_file': 'GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif', 'archive': 'N', 'public': 'Y',
        'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk',
        'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk',
        'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population',
        'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE',
        'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif'],
        'url_img': 'https://www.worldpop.org/tabs/gdata/img/1482/zwe_ppp_wpgp_2000_Image.png',
        'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org',
        'license': 'https://www.worldpop.org/data/licence.txt',
        'url_summary': 'https://www.worldpop.org/geodata/summary?id=1482'}
    """
    json_url = '%s?iso3=%s' % (base_url, iso3)
    response = downloader.download(json_url)
    json = response.json()
    allmetadata = json['data']
    lastmetadata = allmetadata[-1]
    countryname = Country.get_country_name_from_iso3(iso3)
    if not countryname:
        logger.exception('ISO3 %s not recognised!' % iso3)
        return None, None
    title = '%s - %s' % (countryname, indicator['title'])
    slugified_name = slugify('WorldPop %s %s' % (countryname, indicator['title'])).lower()
    logger.info('Creating dataset: %s' % title)
    licence_url = lastmetadata['license'].lower()  # suggest that they remove license and rename this field license
    response = downloader.download(licence_url)
    licence = response.text
    url_summary = lastmetadata['url_summary']
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        'notes': '%s\n%s' % (indicator['desc'], lastmetadata['citation']),
        'methodology': 'Other',
        'methodology_other': lastmetadata['desc'],
        'dataset_source': lastmetadata['source'],
        'license_id': 'hdx-other',
        'license_other': licence,
        'private': False,
        'url': url_summary
    })
    dataset.set_maintainer('37023db4-a571-4f28-8d1f-15f0353586af')
    dataset.set_organization('3f077dff-1d05-484d-a7c2-4cb620f22689')
    dataset.set_expected_update_frequency('Every year')
    dataset.set_subnational(True)
    try:
        dataset.add_country_location(iso3)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None

    tags = [indicator['name'].lower(), 'geodata']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    resources_dict = dict()
    for metadata in json['data']:
        if metadata['public'].lower() != 'y':
            continue
        year = metadata['popyear']
        if not year:
            year = metadata['date'][:4]
        year = int(year)
        if year > latest_year:
            latest_year = year
        if year < earliest_year:
            earliest_year = year
        for url in sorted(metadata['files'], reverse=True):
            resource_name = url[url.rfind('/')+1:]
            description = metadata['title']
            if not re.match(r'.*([1-3][0-9]{3})', resource_name):
                resource_parts = resource_name.split('.')
                resource_name = '%s_%s.%s' % (resource_parts[0], year, resource_parts[1])
                description = '%s in %s' % (description, year)
            resource = {
                'name': resource_name,
                'format': metadata['data_format'],
                'url': url,
                'description': description
            }
            dict_of_lists_add(resources_dict, year, resource)
    for year in sorted(resources_dict.keys(), reverse=True):
        for resource in resources_dict[year]:
            dataset.add_update_resource(resource)

    dataset.set_dataset_year_range(earliest_year, latest_year)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'WorldPop %s %s Summary Page' % (countryname, indicator['title']),
        'notes': 'Takes you to the WorldPop summary page for the %s %s dataset' % (countryname, indicator['title']),
        'url': url_summary,
        'image_url': lastmetadata['url_img']
    })
    showcase.add_tags(tags)
    return dataset, showcase
