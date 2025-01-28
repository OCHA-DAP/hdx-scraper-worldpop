#!/usr/bin/python
"""
WORLDPOP:
------------

Reads WorldPop JSON and creates datasets.

"""

import logging

from dateutil.parser import ParserError
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from markdownify import markdownify
from slugify import slugify

logger = logging.getLogger(__name__)


class WorldPop:
    def __init__(self, retriever: Retrieve, configuration: Configuration):
        self._retriever = retriever
        self._json_url = configuration["json_url"]
        self._indicators = configuration["indicators"]

    def get_indicators_metadata(self):
        json = self._retriever.download_json(self._json_url)
        aliases = list(self._indicators.keys())
        indicators_metadata = {}
        for indicator_metadata in json["data"]:
            alias = indicator_metadata["alias"]
            if alias not in aliases:
                continue
            indicators_metadata[alias] = indicator_metadata
        return indicators_metadata


    def get_countriesdata(self):
        def download(alias, subalias):
            url = f"{self._json_url}{alias}/{subalias}"
            json = self._retriever.download_json(url)

            return url, json["data"]

        countriesdata = {}

        for alias in self._indicators:
            indicators_alias = self._indicators[alias]
            for subalias in indicators_alias.get("country", []):
                url, data = download(alias, subalias)
                iso3s = set()
                for info in data:
                    iso3 = info["iso3"]
                    if iso3 in iso3s:
                        continue
                    iso3s.add(iso3)
                    countrydata = countriesdata.get(iso3, {})
                    countryalias = countrydata.get(alias, {})
                    dict_of_lists_add(countryalias, subalias, f"{url}?iso3={iso3}")
                    countrydata[alias] = countryalias
                    countriesdata[iso3] = countrydata
            subalias = indicators_alias.get("global")
            if subalias:
                url, data = download(alias, subalias)
                countrydata = countriesdata.get("World", {})
                countryalias = countrydata.get(alias, {})
                countryalias[subalias] = [f"{url}?id={x['id']}" for x in data]
                countrydata[alias] = countryalias
                countriesdata["World"] = countrydata

        countries = [{"iso3": x} for x in sorted(countriesdata.keys()) if x != "World"]
        countries.append({"iso3": "World"})
        return countriesdata, countries


    def generate_dataset_and_showcases(
        self, countryiso, indicator_metadata, countryalias
    ):
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
        allmetadata = {}
        for subalias in countryalias:
            urls = countryalias[subalias]
            allmetadata_subalias = allmetadata.get(subalias, [])
            for url in urls:
                json = self._retriever.download_json(url)
                data = json["data"]
                if isinstance(data, list):
                    allmetadata_subalias.extend(data)
                else:
                    allmetadata_subalias.append(data)
            allmetadata[subalias] = allmetadata_subalias
        allmetadatavalues = list(allmetadata.values())
        lastmetadata = allmetadatavalues[0][-1]
        indicator_title = indicator_metadata["title"]
        if countryiso == "World":
            countryname = countryiso
        else:
            countryname = Country.get_country_name_from_iso3(countryiso)
            if not countryname:
                logger.exception(f"ISO3 {countryiso} not recognised!")
                return None, None
        title = f"{countryname} - {indicator_title}"
        slugified_name = slugify(f"WorldPop {indicator_title} for {countryname}").lower()
        logger.info(f"Creating dataset: {title}")
        licence_url = lastmetadata[
            "license"
        ].lower()  # suggest that they remove license and rename this field license
        licence = self._retriever.download_text(licence_url)
        methodology = lastmetadata["desc"].replace("Constrained", "Constrained and unconstrained")
        notes = markdownify(indicator_metadata["desc"])
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
                "notes": notes,
                "caveats": f"Data for earlier dates is available directly from WorldPop  \n  \n{lastmetadata['citation']}",
                "methodology": "Other",
                "methodology_other": methodology,
                "dataset_source": lastmetadata["source"],
                "license_id": "hdx-other",
                "license_other": licence,
                "private": False,
            }
        )
        dataset.set_maintainer("37023db4-a571-4f28-8d1f-15f0353586af")
        dataset.set_organization("3f077dff-1d05-484d-a7c2-4cb620f22689")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        try:
            dataset.add_other_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None

        tags = [indicator_metadata["name"].lower(), "geodata"]
        dataset.add_tags(tags)

        earliest_date = default_enddate
        earliest_year = 10000
        latest_year = 0
        resources_dict = {}
        for subalias in allmetadata:
            for metadata in allmetadata[subalias]:
                if metadata["public"].lower() != "y":
                    continue
                try:
                    date = parse_date(metadata["date"], timezone_handling=3, include_microseconds=True)
                    if date.year < 2000:
                        date = earliest_date
                    elif date < earliest_date:
                        earliest_date = date
                except ParserError:
                    date = earliest_date
                year = metadata["popyear"]
                if not year:
                    year = date.year
                year = int(year)
                if year > latest_year:
                    latest_year = year
                if year < earliest_year:
                    earliest_year = year
                for url in sorted(metadata["files"], reverse=True):
                    resource_name = url[url.rfind("/") + 1 :]
                    description = metadata["category"]
                    resource = Resource(
                        {
                            "name": resource_name,
                            "format": metadata["data_format"],
                            "url": url,
                            "description": description,
                        }
                    )
                    resource.set_date_data_updated(date)
                    dict_of_lists_add(resources_dict, year, resource)
        if not resources_dict:
            logger.error(f"{title} has no data!")
            return None, None
        for year in sorted(resources_dict.keys(), reverse=True):
            for resource in resources_dict[year]:
                dataset.add_update_resource(resource)

        dataset.set_time_period_year_range(earliest_year, latest_year)

        showcases = []
        for i, allmetadatavalue in enumerate(allmetadatavalues):
            lastallmetadatavalue = allmetadatavalue[-1]
            url_img = lastallmetadatavalue["url_img"]
            if not url_img:
                continue
            url_summary = lastallmetadatavalue["url_summary"]
            if i == 0:
                name = f"{slugified_name}-showcase"
            else:
                name = f"{slugified_name}-{i + 1}-showcase"
            showcase = Showcase(
                {
                    "name": name,
                    "title": f"WorldPop {countryname} {indicator_title} Summary Page",
                    "notes": f"Summary for {lastallmetadatavalue['category']} - {countryname}",
                    "url": url_summary,
                    "image_url": url_img,
                }
            )
            showcase.add_tags(tags)
            showcases.append(showcase)
        return dataset, showcases


    def generate_datasets_and_showcases(
        self, countryiso, indicators_metadata, countrydata
    ):
        datasets = []
        showcases = {}
        for alias in countrydata:
            dataset, d_showcases = self.generate_dataset_and_showcases(
                countryiso, indicators_metadata[alias], countrydata[alias]
            )
            if dataset:
                datasets.append(dataset)
                dataset_name = dataset["name"]
                dataset_showcases = showcases.get(dataset_name, [])
                dataset_showcases.extend(d_showcases)
                showcases[dataset_name] = dataset_showcases
        return datasets, showcases
