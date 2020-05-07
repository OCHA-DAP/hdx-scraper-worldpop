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


def get_indicators_metadata(json_url, downloader, global_indicators, country_indicators):
    response = downloader.download(json_url)
    json = response.json()
    aliases = list(global_indicators.keys())
    aliases.extend(country_indicators.keys())
    indicators_metadata = dict()
    for indicator_metadata in json['data']:
        alias = indicator_metadata['alias']
        if alias not in aliases:
            continue
        indicators_metadata[alias] = indicator_metadata
    return indicators_metadata


def get_countriesdata(base_url, downloader, global_indicators, country_indicators):

    def download(alias, lookup):
        subalias = lookup[alias]
        url = '%s%s/%s' % (base_url, alias, subalias)
        response = downloader.download(url)
        json = response.json()
        return url, json['data']

    countriesdata = dict()
    for alias in country_indicators:
        url, data = download(alias, country_indicators)
        iso3s = set()
        for info in data:
            iso3 = info['iso3']
            if iso3 in iso3s:
                continue
            iso3s.add(iso3)
            dict_of_lists_add(countriesdata, iso3, {'alias': alias, 'urls': ['%s?iso3=%s' % (url, iso3)]})

    countries = [{'iso3': x} for x in sorted(countriesdata.keys())]
    for alias in global_indicators:
        url, data = download(alias, global_indicators)
        urls = ['%s?id=%s' % (url, x['id']) for x in data]
        dict_of_lists_add(countriesdata, 'World', {'alias': alias, 'urls': urls})
    if 'World' in countriesdata:
        countries.insert(0, {'iso3': 'World'})

    return countriesdata, countries


def generate_dataset_and_showcase(downloader, countryiso, indicator_metadata, aliasinfo):
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
    allmetadata = list()
    for url in aliasinfo['urls']:
        response = downloader.download(url)
        json = response.json()
        data = json['data']
        if isinstance(data, list):
            allmetadata.extend(data)
        else:
            allmetadata.append(data)
    lastmetadata = allmetadata[-1]
    indicator_title = indicator_metadata['title']
    if countryiso == 'World':
        countryname = countryiso
    else:
        countryname = Country.get_country_name_from_iso3(countryiso)
        if not countryname:
            logger.exception('ISO3 %s not recognised!' % countryiso)
            return None, None
    title = '%s - %s' % (countryname, indicator_title)
    slugified_name = slugify('WorldPop %s for %s' % (indicator_title, countryname)).lower()
    logger.info('Creating dataset: %s' % title)
    licence_url = lastmetadata['license'].lower()  # suggest that they remove license and rename this field license
    response = downloader.download(licence_url)
    licence = response.text
    url_summary = lastmetadata['url_summary']
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
        'notes': '%s\n%s' % (indicator_metadata['desc'], lastmetadata['citation']),
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
        dataset.add_other_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None

    tags = [indicator_metadata['name'].lower(), 'geodata']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    resources_dict = dict()
    for metadata in allmetadata:
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
                resource_name = '%s_%s' % (resource_parts[0], year)
                if len(resource_parts) >= 2:
                    resource_name = '%s.%s' % (resource_name, resource_parts[1])
                description = '%s in %s' % (description, year)
            resource = {
                'name': resource_name,
                'format': metadata['data_format'],
                'url': url,
                'description': description
            }
            dict_of_lists_add(resources_dict, year, resource)
    if not resources_dict:
        logger.error('%s has no data!' % title)
        return None, None
    for year in sorted(resources_dict.keys(), reverse=True):
        for resource in resources_dict[year]:
            dataset.add_update_resource(resource)

    dataset.set_dataset_year_range(earliest_year, latest_year)

    url_img = lastmetadata['url_img']
    if not url_img:
        for lastmetadata in reversed(allmetadata[:-1]):
            url_img = lastmetadata['url_img']
            if url_img:
                break
    if not url_img:
        return dataset, None
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'WorldPop %s %s Summary Page' % (countryname, indicator_title),
        'notes': 'Takes you to the WorldPop summary page for the %s %s dataset' % (countryname, indicator_title),
        'url': url_summary,
        'image_url': lastmetadata['url_img']
    })
    showcase.add_tags(tags)
    return dataset, showcase


def generate_datasets_and_showcases(downloader, countryiso, indicators_metadata, countrydata):
    datasets = list()
    showcases = list()
    for aliasinfo in countrydata:
        dataset, showcase = generate_dataset_and_showcase(downloader, countryiso,
                                                          indicators_metadata[aliasinfo['alias']], aliasinfo)
        if dataset:
            datasets.append(dataset)
            showcases.append(showcase)
    return datasets, showcases
