#!/usr/bin/python
"""
WORLDPOP:
------------

Reads WorldPop API v3 and creates datasets.

"""

import logging
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.scraper.worldpop.dataset_generator import DatasetGenerator
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        retriever: Retrieve,
        hub_retriever: Retrieve,
        configuration: Configuration,
        metadata: Dict,
        year: int,
    ):
        self._retriever = retriever
        self._hub_retriever = hub_retriever
        self._configuration = configuration
        self._base_url = configuration["base_url"]
        self._aliases = configuration["aliases"]
        self._hdx_metadata = metadata
        self._year = year
        self._countriesdata = {}
        self._hub_ids = {}
        Country.countriesdata(include_unofficial=True)

    def get_countriesdata(self) -> Tuple[Dict, List[Dict]]:
        url = f"{self._base_url}countries?years=all"
        json = self._retriever.download_json(url, filename="countries.json")
        for country in json["countries"]:
            iso3 = country["iso3"]
            if all(x["total_population"] is None for x in country["years"]):
                logger.info(f"{iso3} has no data")
                continue
            url = f"{self._base_url}countries/{iso3}/files?years=all"
            files = self._retriever.download_json(url, filename=f"files-{iso3}.json")
            self._countriesdata[iso3] = files
        self.get_hub_ids()
        countries = [{"iso3": x} for x in sorted(self._countriesdata.keys())]
        return self._countriesdata, countries

    def get_hub_ids(self) -> None:
        """Read the WorldPop hub records and map (alias, ISO3, year) to the hub
        record id used in the showcase URL. The hub uses KOS for Kosovo."""
        for alias, aliasinfo in self._aliases.items():
            hub = aliasinfo["hub"]
            url = f"{self._configuration['hub_url']}{hub}"
            filename = f"hub-{alias}.json"
            json = self._hub_retriever.download_json(url, filename=filename)
            for record in json["data"]:
                iso3 = record["iso3"]
                if iso3 == "KOS":
                    iso3 = "XKX"
                self._hub_ids[(alias, iso3, int(record["popyear"]))] = record["id"]

    def get_showcase_url(
        self, alias: str, countryiso3: str, popyear: int, default: str
    ) -> str:
        hub_id = self._hub_ids.get((alias, countryiso3, popyear))
        if not hub_id:
            logger.error(f"No hub record for {countryiso3} {alias} {popyear}!")
            return default
        return self._configuration["hub_summary_url"].format(id=hub_id)

    @staticmethod
    def get_countryname(countryiso3: str) -> Optional[str]:
        countryname = Country.get_country_name_from_iso3(countryiso3)
        if not countryname:
            logger.error(f"ISO3 {countryiso3} not recognised!")
            return None
        return countryname

    @staticmethod
    def get_url(yeardata: Dict, key: str, resolution: str) -> Optional[str]:
        files = yeardata[key]
        if not files:
            return None
        url = files.get(resolution)
        if isinstance(url, dict):  # agesex: {"zip": ..., "bands": {...}}
            url = url["zip"]
        return url

    def generate_datasets_and_showcases(self, countryiso3: str) -> Tuple[List, List]:
        datasets = []
        showcases = []
        countryname = self.get_countryname(countryiso3)
        if not countryname:
            return datasets, showcases
        countrydata = self._countriesdata[countryiso3]
        for alias, hdx_metadata in self._hdx_metadata.items():
            key = self._aliases[alias]["key"]
            resolutions = hdx_metadata["Resolution"]
            years = [x for x in countrydata["years"] if x[key]]
            if not years:
                logger.error(f"{countryname} has no {alias} data!")
                continue
            start_year = years[0]["data_year"]
            end_year = years[-1]["data_year"]
            if start_year <= self._year <= end_year:
                popyear = self._year
            else:
                popyear = end_year
                logger.error(
                    f"{countryname} {alias} does not have data for {self._year}!"
                )
            metadata = {
                "startpopyear": start_year,
                "endpopyear": end_year,
                "popyear": popyear,
                "hdx_metadata": hdx_metadata,
                "data_format": self._aliases[alias]["format"],
                "showcase_url": self.get_showcase_url(
                    alias,
                    countryiso3,
                    popyear,
                    countrydata["citation"][key][resolutions[0]]["url"],
                ),
            }
            dataset_generator = DatasetGenerator(
                self._configuration,
                countryiso3,
                countryname,
                metadata,
            )
            dataset, showcase = dataset_generator.generate_dataset_and_showcase()
            if not dataset:
                continue
            for yeardata in years:
                year = yeardata["data_year"]
                for resolution in resolutions:
                    url = self.get_url(yeardata, key, resolution)
                    if not url:
                        logger.error(
                            f"{countryname} {alias} has no {resolution} data for {year}!"
                        )
                        continue
                    dataset_generator.add_resource_to(dataset, resolution, year, url)
            if len(dataset.get_resources()) == 0:
                logger.error(f"{dataset['title']} has no data!")
            else:
                datasets.append(dataset)
                showcases.append(showcase)
        return datasets, showcases
