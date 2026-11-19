import logging
import re
from typing import Dict

from hdx.api.utilities.date_helper import DateHelper
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        configuration,
        countryiso3,
        countryname,
        metadata,
    ):
        self._configuration = configuration
        self._countryiso3 = countryiso3
        self._countryname = countryname
        self._metadata = metadata

    @staticmethod
    def fill(text: str, **values) -> str:
        """Substitute {name} placeholders, raising if any placeholder is left
        unfilled."""
        if values:
            pattern = r"\{(" + "|".join(map(re.escape, values)) + r")\}"
            text = re.sub(pattern, lambda m: str(values[m.group(1)]), text)
        unfilled = re.findall(r"\{\w+\}", text)
        if unfilled:
            raise ValueError(f"Unfilled placeholders {unfilled} in: {text}")
        return text

    def get_description_values(self) -> Dict[str, str]:
        """Placeholder values for descriptions that come from the API data"""
        resolutions = self._metadata["resolutions"]
        arcs = self._configuration["resolution_arcs"]
        values = {
            "countryname": self._countryname,
            "startyear": self._metadata["startpopyear"],
            "endyear": self._metadata["endpopyear"],
            "release": self._metadata["release"],
            "version": self._metadata["version"],
            "resolutions": " and ".join(resolutions),
            "resolution_details": "; ".join(
                f"{x} is {arcs[x]} arc, approximately {x} at the equator"
                for x in resolutions
            ),
        }
        bands = self._metadata["bands"]
        if bands:
            values["example_file"] = bands[sorted(bands)[0]].rsplit("/", 1)[-1]
        return values

    def generate_dataset_and_showcase(self):
        dataset = Dataset()
        try:
            dataset.add_other_location(self._countryiso3)
        except HDXError as e:
            logger.exception(f"{self._countryiso3} has a problem! {e}")
            return None, None
        hdx_metadata = self._metadata["hdx_metadata"]
        start_year = self._metadata["startpopyear"]
        end_year = self._metadata["endpopyear"]
        title = self.fill(
            hdx_metadata["Dataset Title"],
            countryname=self._countryname,
            startyear=start_year,
            endyear=end_year,
        )
        logger.info(f"Creating dataset: {title}")
        dataset["title"] = title
        name = self.fill(
            hdx_metadata["Dataset Name"],
            startyear=start_year,
            endyear=end_year,
            iso3=self._countryiso3.lower(),
        )
        dataset["name"] = name
        dataset["notes"] = self.fill(
            hdx_metadata["Dataset Description"], **self.get_description_values()
        )
        dataset["dataset_source"] = hdx_metadata["Source"]
        dataset["methodology_other"] = hdx_metadata["Methodology"]
        licence = self._metadata["licence"]
        license_id = self._configuration["licences"].get(licence["name"])
        if not license_id:
            raise ValueError(f"Unrecognised licence {licence['name']}!")
        dataset["license_id"] = license_id
        caveats = hdx_metadata["Caveats"]
        citation = self._metadata["citation"]
        if not citation.strip():
            raise ValueError(f"{self._countryiso3} {title} has an empty citation!")
        if citation.strip() not in caveats:
            caveats = f"{caveats}\n\n{citation.strip()}"
        dataset["caveats"] = caveats
        tags = hdx_metadata["Tags"].split(",")
        dataset.add_tags(tags)
        dataset.set_maintainer("37023db4-a571-4f28-8d1f-15f0353586af")
        dataset.set_organization("3f077dff-1d05-484d-a7c2-4cb620f22689")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        startdate = DateHelper.get_hdx_date(f"{start_year}-01-01", True)
        enddate = DateHelper.get_hdx_date(f"{end_year}-01-01", True)
        dataset["dataset_date"] = f"[{startdate} TO {enddate}]"
        year = self._metadata["popyear"]
        showcase = Showcase(
            {
                "name": f"{name}-showcase",
                "title": self.fill(hdx_metadata["Showcase Title"], year=year),
                "notes": self.fill(
                    hdx_metadata["Showcase Description"],
                    countryname=self._countryname,
                    startyear=start_year,
                    endyear=end_year,
                    release=self._metadata["release"],
                    version=self._metadata["version"],
                ),
                "url": self._metadata["showcase_url"],
                "image_url": self._configuration["showcase_image_url"].format(
                    iso3=self._countryiso3
                ),
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase

    def add_resource_to(self, dataset, resolution, year, url):
        hdx_metadata = self._metadata["hdx_metadata"]
        name = self.fill(
            hdx_metadata["Resource Name"],
            iso3=self._countryiso3.lower(),
            year=year,
            resolution=resolution,
        )
        description = self.fill(
            hdx_metadata["Resource Description"], resolution=resolution, year=year
        )
        resource = Resource(
            {
                "name": name,
                "url": url,
                "description": description,
                "format": self._metadata["data_format"],
            }
        )
        date = parse_date(
            hdx_metadata["Last Modified"],
            timezone_handling=3,
            include_microseconds=True,
        )
        resource.set_date_data_updated(date)
        dataset.add_update_resource(resource)
