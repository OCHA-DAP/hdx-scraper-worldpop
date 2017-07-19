#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for worldpop.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from worldpop import generate_dataset_and_showcase, get_countriesdata


class TestWorldPop:
    countrydata = {'Source': 'WorldPop, University of Southampton, UK',
                   'URL_image': 'http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP_500.JPG',
                   'fileFormat': 'zipped geotiff',
                   'Dataset Title': 'WorldPop Zimbabwe Population dataset',
                   'authorEmail': 'worldpop@geodata.soton.ac.uk',
                   'updateFrequency': 'Every year', 'id_no': '243', 'authorName': 'WorldPop',
                   'URL_datasetDetailsPage': 'http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP.txt',
                   'Description': 'These datasets provide estimates of population counts ... information.',
                   'Visibility': 'Public',
                   'tags': ['Population Statistics', 'WorldPop', 'University of Southampton'],
                   'lastModifiedDate': '2016-10-17T12:54:54+01:00',
                   'datasetDatePrecision': 'Year',
                   'URL_summaryPage': 'http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population',
                   'URL_direct': 'http://www.worldpop.org.uk/data/hdx/?dataset=ZWE-POP',
                   'Dataset contains sub-national data': 'true',
                   'Organisation': 'WorldPop, University of Southampton, UK; www.worldpop.org',
                   'datasetDate': '2015-01-01T00:00:00+00:00',
                   'productionDate': '2013-01-01T00:00:00+00:00',
                   'License': 'Other',
                   'Define License': 'http://www.worldpop.org.uk/data/licence.txt',
                   'location': 'ZWE',
                   'maintainerName': 'WorldPop',
                   'Location': 'Zimbabwe',
                   'maintainerEmail': 'worldpop@geodata.soton.ac.uk'}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    @pytest.fixture(scope='function')
    def downloader(self):
        class Request:
            def json(self):
                pass

        class Download:
            @staticmethod
            def download(url):
                request = Request()
                if url == 'http://lala/getJSON/':
                    def fn():
                        return {'worldPopData': [TestWorldPop.countrydata]}
                    request.json = fn
                elif url == 'http://www.worldpop.org.uk/data/licence.txt':
                    request.text = 'The WorldPop project aims to provide an open access archive of spatial ' \
                                   'demographic datasets ... at creativecommons.org.'
                return request
        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://lala/getJSON/', downloader)
        assert countriesdata == [TestWorldPop.countrydata]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        dataset, showcase = generate_dataset_and_showcase(downloader, TestWorldPop.countrydata)
        assert dataset == {'dataset_source': 'WorldPop, University of Southampton, UK',
                           'notes': 'These datasets provide estimates of population counts ... information.',
                           'data_update_frequency': '365',
                           'name': 'worldpop-zimbabwe-population',
                           'author': 'WorldPop',
                           'license_other': 'The WorldPop project aims to provide an open access archive of spatial demographic datasets ... at creativecommons.org.',
                           'license_id': 'hdx-other',
                           'dataset_date': '01/01/2015',
                           'url': 'http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population',
                           'tags': [{'name': 'Population Statistics'}, {'name': 'WorldPop'}, {'name': 'University of Southampton'}],
                           'subnational': False,
                           'groups': [{'name': 'zwe'}],
                           'maintainer': 'WorldPop',
                           'methodology_other': 'Go to [WorldPop Dataset Summary Page](http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population) for more information',
                           'private': False,
                           'methodology': 'Other',
                           'maintainer_email': 'worldpop@geodata.soton.ac.uk',
                           'title': 'WorldPop Zimbabwe Population',
                           'author_email': 'worldpop@geodata.soton.ac.uk'}

        resources = dataset.get_resources()
        assert resources == [{'description': 'Go to [WorldPop Dataset Summary Page](http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population) for more information',
                              'resource_type': 'api', 'format': 'zipped geotiff',
                              'url': 'http://www.worldpop.org.uk/data/hdx/?dataset=ZWE-POP', 'url_type': 'api',
                              'name': 'WorldPop Zimbabwe Population'}]

        assert showcase == {'image_url': 'http://www.worldpop.org.uk/data/WorldPop_data/AllContinents/ZWE-POP_500.JPG',
                            'name': 'worldpop-zimbabwe-population-showcase',
                            'title': 'WorldPop Zimbabwe Summary Page',
                            'notes': 'Click the image on the right to go to the WorldPop summary page for the Zimbabwe dataset',
                            'url': 'http://www.worldpop.org.uk/data/summary?contselect=Africa&countselect=Zimbabwe&typeselect=Population',
                            'tags': [{'name': 'Population Statistics'}, {'name': 'WorldPop'}, {'name': 'University of Southampton'}]}

