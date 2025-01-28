#!/usr/bin/python
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

from hdx.api.configuration import Configuration
from hdx.scraper.worldpop.aliasdata import AliasData
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, retriever: Retrieve, configuration: Configuration):
        self._retriever = retriever
        self._json_url = configuration["json_url"]
        self._indicators = configuration["indicators"]
        self._indicators_metadata = {}
        self._countriesdata = {}

    def get_indicators_metadata(self):
        json = self._retriever.download_json(self._json_url)
        aliases = list(self._indicators.keys())
        for indicator_metadata in json["data"]:
            alias = indicator_metadata["alias"]
            if alias not in aliases:
                continue
            self._indicators_metadata[alias] = indicator_metadata
        return self._indicators_metadata

    def get_countriesdata(self):
        def download(alias, subalias):
            url = f"{self._json_url}{alias}/{subalias}"
            json = self._retriever.download_json(url)

            return url, json["data"]

        for alias, indicators_alias in self._indicators.items():
            for subalias in indicators_alias:
                url, data = download(alias, subalias)
                iso3s = set()
                for info in data:
                    iso3 = info["iso3"]
                    if iso3 in iso3s:
                        continue
                    iso3s.add(iso3)
                    countrydata = self._countriesdata.get(iso3, {})
                    countryalias = countrydata.get(alias, {})
                    dict_of_lists_add(
                        countryalias, subalias, f"{url}?iso3={iso3}"
                    )
                    countrydata[alias] = countryalias
                    self._countriesdata[iso3] = countrydata

        countries = [
            {"iso3": x}
            for x in sorted(self._countriesdata.keys())
        ]
        return self._countriesdata, countries

    def generate_all_datasets_and_showcases(self, countryiso3):
        all_datasets = []
        all_showcases = {}
        for alias, countrydata in self._countriesdata[countryiso3].items():
            aliasdata = AliasData(
                self._retriever,
                countryiso3,
                self._indicators_metadata[alias],
                countrydata,
            )
            success = aliasdata.set_country_name()
            if not success:
                continue
            aliasdata.get_allmetadata_for_country()
            datasets = aliasdata.generate_datasets()
            if not datasets:
                continue
            all_datasets.extend(datasets)
            all_showcases.update(aliasdata.get_showcases())
        return all_datasets, all_showcases
