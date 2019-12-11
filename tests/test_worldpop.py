#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for worldpop.

'''
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from worldpop import generate_dataset_and_showcase, get_indicatorsdata, get_url_iso3s


class TestWorldPop:
    indicatorsdata = [{'alias': 'pop', 'name': 'Population', 'title': 'Population', 'desc': 'WorldPop produces different types of gridded population count datasets...'},
                      {'alias': 'births', 'name': 'Births', 'title': 'Births', 'desc': 'The health and survival of women and their new-born babies in low income countries is a key public health priority...'},
                      {'alias': 'pregnancies', 'name': 'Pregnancies', 'title': 'Pregnancies', 'desc': 'The health and survival of women and their new-born babies in low income countries is a key public health priority...'},
                      {'alias': 'urban_change', 'name': 'Urban change', 'title': 'Urban change', 'desc': 'East–Southeast Asia is currently one of the fastest urbanizing regions...'},
                      {'alias': 'age_structures', 'name': 'Age and sex structures', 'title': 'Age and sex structures', 'desc': 'Age and sex structures: WorldPop produces different types of gridded population count datasets...'},
                      {'alias': 'dahi', 'name': 'Development Indicators', 'title': 'Development and Health Indicators', 'desc': 'Improved understanding of geographical variation and inequity...'}]
    urlsdata = [{'alias': 'pic', 'name': 'Individual countries'}, {'alias': '', 'name': 'Whole Continent'}, {'alias': 'wpgp', 'name': 'Global per country 2000-2020'}]
    isosdata = [{'id': '1325', 'iso3': 'AUS'}, {'id': '1326', 'iso3': 'RUS'}, {'id': '1327', 'iso3': 'BRA'}, {'id': '1328', 'iso3': 'CAN'}, {'id': '1482', 'iso3': 'ZWE'}]
    metadata = [{'id': '1482', 'title': 'The spatial distribution of population in 2000, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2000', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/1482/zwe_ppp_wpgp_2000_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=1482'},
                {'id': '1731', 'title': 'The spatial distribution of population in 2001, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2001', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/1731/zwe_ppp_wpgp_2001_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=1731'},
                {'id': '3474', 'title': 'The spatial distribution of population in 2008, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2008', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/3474/zwe_ppp_wpgp_2008_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=3474'},
                {'id': '4711', 'title': 'The spatial distribution of population in 2013, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2013', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/4711/zwe_ppp_wpgp_2013_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=4711'},
                {'id': '6205', 'title': 'The spatial distribution of population in 2019, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2019', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/6205/zwe_ppp_wpgp_2019_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=6205'},
                {'id': '6454', 'title': 'The spatial distribution of population in 2020, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2020', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=6454'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'zwe', 'title': 'Zimbabwe'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'population'}, {'name': 'geodata'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/getJSON/':
                    def fn():
                        return {'data': TestWorldPop.indicatorsdata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop':
                    def fn():
                        return {'data': TestWorldPop.urlsdata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp':
                    def fn():
                        return {'data': TestWorldPop.isosdata}
                    response.json = fn
                elif url == 'http://haha/getJSON/pop/wpgp?iso3=ZWE':
                    def fn():
                        return {'data': TestWorldPop.metadata}
                    response.json = fn
                elif url == 'https://www.worldpop.org/data/licence.txt':
                    response.text = 'The WorldPop project aims to provide an open access archive of spatial ' \
                                   'demographic datasets ... at creativecommons.org.'
                return response
        return Download()

    def test_get_indicatorsdata(self, downloader):
        indicatorsdata = get_indicatorsdata('http://lala/getJSON/', downloader)
        assert 'pop' in indicatorsdata.keys()
        assert sorted(list(indicatorsdata.values()), key=lambda k: k['alias']) == sorted(TestWorldPop.indicatorsdata, key=lambda k: k['alias'])

    def test_get_url_iso3s(self, downloader):
        base_url, iso3sdata = get_url_iso3s('http://papa/getJSON/', downloader, 'pop')
        assert base_url == 'http://papa/getJSON/pop/wpgp'
        assert sorted(list(iso3sdata.values()), key=lambda k: k['id']) == sorted(TestWorldPop.isosdata, key=lambda k: k['id'])

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        dataset, showcase = generate_dataset_and_showcase(downloader, 'http://haha/getJSON/pop/wpgp', TestWorldPop.indicatorsdata[0], 'ZWE')
        assert dataset == {'name': 'worldpop-zimbabwe-population', 'title': 'Zimbabwe - Population',
                           'notes': 'WorldPop produces different types of gridded population count datasets...\nWorldPop',
                           'methodology': 'Other', 'methodology_other': 'Estimated total number of people per grid-cell.',
                           'dataset_source': 'WorldPop, University of Southampton, UK', 'license_id': 'hdx-other',
                           'license_other': 'The WorldPop project aims to provide an open access archive of spatial demographic datasets ... at creativecommons.org.',
                           'private': False, 'url': 'https://www.worldpop.org/geodata/summary?id=6454',
                           'maintainer': '37023db4-a571-4f28-8d1f-15f0353586af', 'owner_org': '3f077dff-1d05-484d-a7c2-4cb620f22689',
                           'data_update_frequency': '365', 'subnational': '1', 'groups': [{'name': 'zwe'}],
                           'tags': [{'name': 'population', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                    {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                           'dataset_date': '01/01/2000-12/31/2020'}

        resources = dataset.get_resources()
        assert resources == [{'name': 'zwe_ppp_2020.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif',
                              'description': 'The spatial distribution of population in 2020, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2019.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif',
                              'description': 'The spatial distribution of population in 2019, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2013.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif',
                              'description': 'The spatial distribution of population in 2013, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2008.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif',
                              'description': 'The spatial distribution of population in 2008, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2001.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif',
                              'description': 'The spatial distribution of population in 2001, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2000.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif',
                              'description': 'The spatial distribution of population in 2000, Zimbabwe',
                              'resource_type': 'api', 'url_type': 'api'}]

        assert showcase == {'name': 'worldpop-zimbabwe-population-showcase',
                            'title': 'WorldPop Zimbabwe Population Summary Page',
                            'notes': 'Takes you to the WorldPop summary page for the Zimbabwe Population dataset',
                            'url': 'https://www.worldpop.org/geodata/summary?id=6454',
                            'image_url': 'https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png',
                            'tags': [{'name': 'population', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                     {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}


