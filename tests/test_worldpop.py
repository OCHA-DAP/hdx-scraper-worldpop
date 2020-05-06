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

from worldpop import generate_datasets_and_showcases, get_indicators_metadata, get_countriesdata


class TestWorldPop:
    indicators_metadata = [{'alias': 'pop', 'name': 'Population', 'title': 'Population', 'desc': 'WorldPop produces different types of gridded population count datasets...'},
                           {'alias': 'births', 'name': 'Births', 'title': 'Births', 'desc': 'The health and survival of women and their new-born babies in low income countries is a key public health priority...'},
                           {'alias': 'pregnancies', 'name': 'Pregnancies', 'title': 'Pregnancies', 'desc': 'The health and survival of women and their new-born babies in low income countries is a key public health priority...'},
                           {'alias': 'age_structures', 'name': 'Age and sex structures', 'title': 'Age and sex structures', 'desc': 'Age and sex structures: WorldPop produces different types of gridded population count datasets...'}]
    countriesdata = {'AUS': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp?iso3=AUS']}], 'RUS': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp?iso3=RUS']}],
                     'BRA': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp?iso3=BRA']}], 'CAN': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp?iso3=CAN']}],
                     'ZWE': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp?iso3=ZWE']}], 'World': [{'alias': 'pop', 'urls': ['http://papa/getJSON/pop/wpgp1km?id=24776', 'http://papa/getJSON/pop/wpgp1km?id=24777']}]}
    wpgpdata = [{'id': '1325', 'iso3': 'AUS'}, {'id': '1326', 'iso3': 'RUS'}, {'id': '1327', 'iso3': 'BRA'}, {'id': '1328', 'iso3': 'CAN'}, {'id': '1482', 'iso3': 'ZWE'}]
    metadata = [{'id': '1482', 'title': 'The spatial distribution of population in 2000, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2000', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/1482/zwe_ppp_wpgp_2000_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=1482'},
                {'id': '1731', 'title': 'The spatial distribution of population in 2001, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2001', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/1731/zwe_ppp_wpgp_2001_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=1731'},
                {'id': '3474', 'title': 'The spatial distribution of population in 2008, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2008', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/3474/zwe_ppp_wpgp_2008_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=3474'},
                {'id': '4711', 'title': 'The spatial distribution of population in 2013, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2013', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/4711/zwe_ppp_wpgp_2013_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=4711'},
                {'id': '6205', 'title': 'The spatial distribution of population in 2019, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2019', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/6205/zwe_ppp_wpgp_2019_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=6205'},
                {'id': '6454', 'title': 'The spatial distribution of population in 2020, Zimbabwe', 'desc': 'Estimated total number of people per grid-cell.', 'doi': '10.5258/SOTON/WP00645', 'date': '2018-11-01', 'popyear': '2020', 'citation': 'WorldPop', 'data_file': 'GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'Geotiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global per country 2000-2020', 'gtype': 'Population', 'continent': 'Africa', 'country': 'Zimbabwe', 'iso3': 'ZWE', 'files': ['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=6454'}]
    wpgp1kmdata = [{'id': '24776'}, {'id': '24777'}]
    metadata_24777 = {'id': '24777', 'title': 'The spatial distribution of population in 2020', 'desc': 'Estimated total number of people per grid-cell...\r\n', 'doi': '10.5258/SOTON/WP00647', 'date': '0018-02-01', 'popyear': '2020', 'citation': 'WorldPop...\r\n', 'data_file': 'GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif', 'file_img': 'world_ppp_wpgp_2020_Image.png', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'tiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global mosaics 2000-2020', 'gtype': 'Population', 'continent': 'World', 'country': None, 'iso3': None, 'files':['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/24777/world_ppp_wpgp_2020_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=24777'}
    metadata_24776 = {'id': '24776', 'title': 'The spatial distribution of population in 2019', 'desc': 'Estimated total number of people per grid-cell...\r\n', 'doi': '10.5258/SOTON/WP00647', 'date': '2018-11-01', 'popyear': '2019', 'citation': 'WorldPop...\r\n', 'data_file': 'GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif', 'file_img': 'world_ppp_wpgp_2019_Image.png', 'archive': 'N', 'public': 'Y', 'source': 'WorldPop, University of Southampton, UK', 'data_format': 'tiff', 'author_email': 'wp@worldpop.uk', 'author_name': 'WorldPop', 'maintainer_name': 'WorldPop', 'maintainer_email': 'wp@worldpop.uk', 'project': 'Population', 'category': 'Global mosaics 2000-2020', 'gtype': 'Population', 'continent': 'World', 'country': None, 'iso3': None, 'files':['ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif'], 'url_img': 'https://www.worldpop.org/tabs/gdata/img/24776/world_ppp_wpgp_2019_Image.png', 'organisation': 'WorldPop, University of Southampton, UK, www.worldpop.org', 'license': 'https://www.worldpop.org/data/licence.txt', 'url_summary': 'https://www.worldpop.org/geodata/summary?id=24776'}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'zwe', 'title': 'Zimbabwe'}, {'name': 'world', 'title': 'World'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'population'}, {'name': 'geodata'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}
        return Configuration.read()

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
                        return {'data': TestWorldPop.indicators_metadata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp':
                    def fn():
                        return {'data': TestWorldPop.wpgpdata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp1km':
                    def fn():
                        return {'data': TestWorldPop.wpgp1kmdata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp?iso3=ZWE':
                    def fn():
                        return {'data': TestWorldPop.metadata}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp1km?id=24776':
                    def fn():
                        return {'data': TestWorldPop.metadata_24776}
                    response.json = fn
                elif url == 'http://papa/getJSON/pop/wpgp1km?id=24777':
                    def fn():
                        return {'data': TestWorldPop.metadata_24777}
                    response.json = fn
                elif url == 'https://www.worldpop.org/data/licence.txt':
                    response.text = 'The WorldPop project aims to provide an open access archive of spatial ' \
                                   'demographic datasets ... at creativecommons.org.'
                return response
        return Download()

    def test_get_indicators_metadata(self, configuration, downloader):
        country_indicators = configuration['country_indicators']
        global_indicators = configuration['global_indicators']
        indicators_metadata = get_indicators_metadata('http://lala/getJSON/', downloader, global_indicators, country_indicators)
        assert 'pop' in indicators_metadata.keys()
        assert sorted(list(indicators_metadata.values()), key=lambda k: k['alias']) == sorted(TestWorldPop.indicators_metadata, key=lambda k: k['alias'])

    def test_get_countriesdata(self, downloader):
        countriesdata, countries = get_countriesdata('http://papa/getJSON/', downloader, {'pop': 'wpgp1km'}, {'pop': 'wpgp'})
        assert countriesdata == TestWorldPop.countriesdata
        assert countries == [{'iso3': 'World'}, {'iso3': 'AUS'}, {'iso3': 'BRA'}, {'iso3': 'CAN'}, {'iso3': 'RUS'}, {'iso3': 'ZWE'}]

    def test_generate_datasets_and_showcases(self, configuration, downloader):
        indicators_metadata = {'pop': TestWorldPop.indicators_metadata[0]}
        countryiso = 'World'
        countrydata = TestWorldPop.countriesdata[countryiso]
        datasets, showcases = generate_datasets_and_showcases(downloader, countryiso, indicators_metadata, countrydata)
        dataset = datasets[0]
        assert dataset == {'name': 'worldpop-population-for-world', 'title': 'World - Population',
                           'notes': 'WorldPop produces different types of gridded population count datasets...\nWorldPop...\r\n',
                           'methodology': 'Other', 'methodology_other': 'Estimated total number of people per grid-cell...\r\n',
                           'dataset_source': 'WorldPop, University of Southampton, UK', 'license_id': 'hdx-other',
                           'license_other': 'The WorldPop project aims to provide an open access archive of spatial demographic datasets ... at creativecommons.org.',
                           'private': False, 'url': 'https://www.worldpop.org/geodata/summary?id=24777',
                           'maintainer': '37023db4-a571-4f28-8d1f-15f0353586af', 'owner_org': '3f077dff-1d05-484d-a7c2-4cb620f22689',
                           'data_update_frequency': '365', 'subnational': '1', 'groups': [{'name': 'world'}],
                           'tags': [{'name': 'population', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                    {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                           'dataset_date': '01/01/2019-12/31/2020'}

        resources = dataset.get_resources()
        assert resources == [{'name': 'ppp_2020_1km_Aggregated.tif', 'format': 'tiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif',
                              'description': 'The spatial distribution of population in 2020', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'ppp_2019_1km_Aggregated.tif', 'format': 'tiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif',
                              'description': 'The spatial distribution of population in 2019', 'resource_type': 'api', 'url_type': 'api'}]

        showcase = showcases[0]
        assert showcase == {'name': 'worldpop-population-for-world-showcase',
                            'title': 'WorldPop World Population Summary Page',
                            'notes': 'Takes you to the WorldPop summary page for the World Population dataset',
                            'url': 'https://www.worldpop.org/geodata/summary?id=24777',
                            'image_url': 'https://www.worldpop.org/tabs/gdata/img/24777/world_ppp_wpgp_2020_Image.png',
                            'tags': [{'name': 'population', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                     {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

        countryiso = 'ZWE'
        countrydata = TestWorldPop.countriesdata[countryiso]
        datasets, showcases = generate_datasets_and_showcases(downloader, countryiso, indicators_metadata, countrydata)
        dataset = datasets[0]
        assert dataset == {'name': 'worldpop-population-for-zimbabwe', 'title': 'Zimbabwe - Population',
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
                              'description': 'The spatial distribution of population in 2020, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2019.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif',
                              'description': 'The spatial distribution of population in 2019, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2013.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif',
                              'description': 'The spatial distribution of population in 2013, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2008.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif',
                              'description': 'The spatial distribution of population in 2008, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2001.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif',
                              'description': 'The spatial distribution of population in 2001, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'},
                             {'name': 'zwe_ppp_2000.tif', 'format': 'Geotiff',
                              'url': 'ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif',
                              'description': 'The spatial distribution of population in 2000, Zimbabwe', 'resource_type': 'api', 'url_type': 'api'}]

        showcase = showcases[0]
        assert showcase == {'name': 'worldpop-population-for-zimbabwe-showcase',
                            'title': 'WorldPop Zimbabwe Population Summary Page',
                            'notes': 'Takes you to the WorldPop summary page for the Zimbabwe Population dataset',
                            'url': 'https://www.worldpop.org/geodata/summary?id=6454',
                            'image_url': 'https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png',
                            'tags': [{'name': 'population', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                     {'name': 'geodata', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

