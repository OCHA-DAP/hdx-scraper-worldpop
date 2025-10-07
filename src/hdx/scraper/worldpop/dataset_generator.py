import logging
import re

from hdx.location.country import Country

from hdx.api.utilities.date_helper import DateHelper
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dateparse import parse_date
from hdx.utilities.matching import multiple_replace

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        retriever,
        configuration,
        countryiso3,
        countryname,
        metadata,
    ):
        self._retriever = retriever
        self._configuration = configuration
        self._countryiso3 = countryiso3
        self._countryname = countryname
        self._metadata = metadata
        self._showcase = {}
        self._resource_description = None

    def get_showcase(self):
        return self._showcase

    def generate_dataset_and_showcase(self):
        dataset = Dataset()
        try:
            dataset.add_other_location(self._countryiso3)
        except HDXError as e:
            logger.exception(f"{self._countryiso3} has a problem! {e}")
            return None, None
        hdx_metadata = self._metadata["hdx_metadata"]
        countryname = Country.get_country_name_from_iso3(self._countryiso3)
        start_year = self._metadata["startpopyear"]
        end_year = self._metadata["endpopyear"]
        title = hdx_metadata["Dataset Title"].format(countryname=countryname, startyear=start_year, endyear=end_year)
        logger.info(f"Creating dataset: {title}")
        dataset["title"] = title
        name = hdx_metadata["Dataset Name"].format(startyear=start_year, endyear=end_year, iso3=self._countryiso3.lower())
        dataset["name"] = name
        dataset["notes"] = hdx_metadata["Dataset Description"]
        dataset["dataset_source"] = hdx_metadata["Source"]
        dataset["methodology_other"] = hdx_metadata["Methodology"]
        dataset["license_other"] = hdx_metadata["Licence"]
        dataset["caveats"] = hdx_metadata["Caveats"]
        tags = hdx_metadata["Tags"].split(",")
        dataset.add_tags(tags)
        dataset.set_maintainer("37023db4-a571-4f28-8d1f-15f0353586af")
        dataset.set_organization("3f077dff-1d05-484d-a7c2-4cb620f22689")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        startdate = DateHelper.get_hdx_date(f"{start_year}-01-01", True)
        enddate = DateHelper.get_hdx_date(f"{end_year}-01-01", True)
        dataset["dataset_date"] = f"[{startdate} TO {enddate}]"
        url_img = self._metadata["url_img"]
        if url_img:
            url_summary = self._metadata["url_summary"]
            year=self._metadata["popyear"]
            showcase = Showcase(
                {
                    "name": f"{name}-showcase",
                    "title": hdx_metadata["Showcase Title"].format(year=year),
                    "notes": hdx_metadata["Showcase Description"].format(countryname=countryname, startyear=start_year, endyear=end_year),
                    "url": url_summary,
                    "image_url": url_img,
                }
            )
            showcase.add_tags(tags)
        else:
            showcase = None
        return dataset, showcase

    def add_resource_to(self, resolution, dataset, metadata):
        hdx_metadata = self._metadata["hdx_metadata"]
        iso3 = metadata["iso3"]
        year = metadata["popyear"]
        for url in sorted(metadata["files"], reverse=True):
            name = hdx_metadata["Resource Name"].format(iso3=iso3.lower(), year=year, resolution=resolution)
            description = hdx_metadata["Resource Description"].format(resolution=resolution, year=year)
            data_format = metadata["data_format"]
            resource = Resource(
                {
                    "name": name,
                    "url": url,
                    "description": description,
                    "format": data_format,
                }
            )
            date = parse_date(
                hdx_metadata["Last Modified"],
                timezone_handling=3,
                include_microseconds=True,
            )

            resource.set_date_data_updated(date)
            dataset.add_update_resource(resource)
