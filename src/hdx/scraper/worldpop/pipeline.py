#!/usr/bin/python
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.scraper.worldpop.aliasdata import AliasData
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, retriever: Retrieve, configuration: Configuration, year: int):
        self._retriever = retriever
        self._configuration = configuration
        self._year = year
        self._json_url = configuration["json_url"]
        self._indicators = configuration["indicators"]
        self._indicators_metadata = {}
        self._countriesdata = {}
        Country.countriesdata(include_unofficial=True)

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
        def download(alias, indicator):
            url = f"{self._json_url}{alias}/{indicator}"
            json = self._retriever.download_json(url)

            return url, json["data"]

        for alias, indicator in self._indicators.items():
            url, data = download(alias, indicator)
            iso3s = set()
            for info in data:
                iso3 = info["iso3"]
                if iso3 == "KOS":  # remap Kosovo
                    iso3 = "XKX"
                    url_iso3 = "KOS"
                else:
                    url_iso3 = iso3
                if iso3 in iso3s:
                    continue
                iso3s.add(iso3)
                countrydata = self._countriesdata.get(iso3, {})
                countrydata[alias] = f"{url}?iso3={url_iso3}"
                self._countriesdata[iso3] = countrydata

        countries = [{"iso3": x} for x in sorted(self._countriesdata.keys())]
        return self._countriesdata, countries

    @staticmethod
    def get_countryname(countryiso3):
        if countryiso3 == "World":
            return countryiso3
        else:
            countryname = Country.get_country_name_from_iso3(countryiso3)
            if not countryname:
                logger.exception(f"ISO3 {countryiso3} not recognised!")
                return None
            return countryname

    def generate_datasets_and_showcases(self, countryiso3):
        datasets = []
        showcases = []
        countryname = self.get_countryname(countryiso3)
        if not countryname:
            return datasets, showcases
        for alias, country_url in self._countriesdata[countryiso3].items():
            metadata_allyears = self._retriever.download_json(country_url)["data"]
            # We're going to take this year's metadata and make it for all
            # years since we're making one dataset
            start_year = metadata_allyears[0]["popyear"]
            index = self._year - int(start_year)
            num_years = len(metadata_allyears)
            if num_years > index:
                metadata = metadata_allyears[self._year - int(start_year)]
            else:
                metadata = metadata_allyears[num_years - 1]
                logger.error(
                    f"{countryname} {alias} does not have data for {self._year}!"
                )

            # Assume that if one year is excluded, then the whole alias is out
            if metadata["public"].lower() != "y":
                continue
            metadata["startpopyear"] = start_year
            metadata["endpopyear"] = metadata_allyears[-1]["popyear"]
            metadata["alias"] = alias
            aliasdata = AliasData(
                self._retriever,
                self._configuration,
                countryiso3,
                countryname,
                metadata,
            )
            dataset, showcase = aliasdata.generate_dataset_and_showcase()
            if not dataset:
                continue
            for metadata in metadata_allyears:
                aliasdata.add_resource_to(dataset, metadata)
            if len(dataset.get_resources()) == 0:
                logger.error(f"{dataset['title']} has no data!")
            else:
                datasets.append(dataset)
                showcases.append(showcase)
        return datasets, showcases
