#!/usr/bin/python
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging
from typing import Dict

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.scraper.worldpop.dataset_generator import DatasetGenerator
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        retriever: Retrieve,
        configuration: Configuration,
        metadata: Dict,
        year: int,
    ):
        self._retriever = retriever
        self._configuration = configuration
        self._hdx_metadata = metadata
        self._year = year
        self._countriesdata = {}
        Country.countriesdata(include_unofficial=True)

    def get_countriesdata(self):
        json_url = self._configuration["json_url"]

        def download(alias, indicator):
            url = f"{json_url}{alias}/{indicator}"
            json = self._retriever.download_json(url, filename=f"{indicator}.json")

            return url, json["data"]

        for alias, metadata in self._hdx_metadata.items():
            for i, resolution in enumerate(metadata["Resolution"]):
                indicator = metadata["Indicator"][i]
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
                    aliasdata = countrydata.get(alias, {})
                    aliasdata[resolution] = f"{url}?iso3={url_iso3}"
                    countrydata[alias] = aliasdata
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
        for alias, country_urls in self._countriesdata[countryiso3].items():
            resolution_country_urls = list(country_urls.items())
            resolution, country_url = resolution_country_urls[0]
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

            # Assume that if one year is excluded, then there is no data for the
            # indicator
            if metadata["public"].lower() != "y":
                continue
            metadata["startpopyear"] = start_year
            metadata["endpopyear"] = metadata_allyears[-1]["popyear"]
            metadata["hdx_metadata"] = self._hdx_metadata[alias]
            dataset_generator = DatasetGenerator(
                self._retriever,
                self._configuration,
                countryiso3,
                countryname,
                metadata,
            )
            dataset, showcase = dataset_generator.generate_dataset_and_showcase()
            if not dataset:
                continue
            for metadata in metadata_allyears:
                dataset_generator.add_resource_to(resolution, dataset, metadata)
            for resolution, country_url in resolution_country_urls[1:]:
                metadata_allyears = self._retriever.download_json(country_url)["data"]
                for metadata in metadata_allyears:
                    dataset_generator.add_resource_to(resolution, dataset, metadata)
            if len(dataset.get_resources()) == 0:
                logger.error(f"{dataset['title']} has no data!")
            else:
                datasets.append(dataset)
                showcases.append(showcase)
        return datasets, showcases
