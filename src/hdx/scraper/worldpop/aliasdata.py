import logging
import re

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_filename_extension_from_url

logger = logging.getLogger(__name__)


class AliasData:
    distance_regex = re.compile(r"_\d*m")

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
        self._resource_base_description = None

    def get_showcase(self):
        return self._showcase

    def get_tags(self):
        return [self._metadata["project"].lower(), "geodata"]

    def get_caveats(self, disclaimer):
        citation = self._metadata["citation"]
        return f"{disclaimer}{self._configuration['caveat_prefix']}{citation}"

    def replace_countryname(self, title):
        title = title.split(" - ")
        return f"{self._countryname} - {title[1]}"

    def generate_dataset_and_showcase(self):
        dataset = Dataset()
        try:
            dataset.add_other_location(self._countryiso3)
        except HDXError as e:
            logger.exception(f"{self._countryiso3} has a problem! {e}")
            return None, None
        project = self._metadata["project"]
        category = self._metadata["category"]
        title = self.replace_countryname(self._metadata["title"])
        estimate_type, _ = category.split(maxsplit=1)
        start_year = self._metadata["startpopyear"]
        end_year = self._metadata["endpopyear"]
        year_range = f"{start_year}-{end_year}"
        title = f"{title} ({year_range})"
        name = slugify(f"worldpop-{project}-{year_range}-{self._countryiso3}")
        logger.info(f"Creating dataset: {title}")
        dataset["name"] = name
        dataset["title"] = title
        notes_suffix = self._configuration["notes_suffix"]
        year = self._metadata["popyear"]
        desc = (
            self._metadata["desc"]
            .replace(f" in {year}", "")
            .replace(f" of {year}", "")
        )
        disclaimer = desc.split("Disclaimer", maxsplit=1)
        if len(disclaimer) == 2:
            desc = disclaimer[0]
            disclaimer = f"Disclaimer{disclaimer[1]}  \n  \n"
        else:
            disclaimer = ""
        notes = desc.split("File Descriptions:", maxsplit=1)
        dataset["notes"] = (
            f"{notes[0]}{notes_suffix['global']}{notes_suffix.get(self._metadata["alias"], '')}"
        )
        dataset["caveats"] = self.get_caveats(disclaimer)
        dataset.set_maintainer("37023db4-a571-4f28-8d1f-15f0353586af")
        dataset.set_organization("3f077dff-1d05-484d-a7c2-4cb620f22689")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        dataset.set_time_period_year_range(start_year, end_year)
        dataset.add_tags(self.get_tags())

        bracketed_text = category.split("(", 1)[1].split(")")[0].strip()
        self._resource_base_description = (
            f"{estimate_type} {project.lower()} ({bracketed_text}) for "
        )

        url_img = self._metadata["url_img"]
        if url_img:
            url_summary = self._metadata["url_summary"]
            showcase = Showcase(
                {
                    "name": f"{name}-showcase",
                    "title": f"{year} {project}",
                    "notes": f"{self._countryname} WorldPop summary: {category}",
                    "url": url_summary,
                    "image_url": url_img,
                }
            )
            showcase.add_tags(self.get_tags())
        else:
            showcase = None
        return dataset, showcase

    def add_resource_to(self, dataset, metadata):
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
        date = parse_date(
            metadata["date"],
            timezone_handling=3,
            include_microseconds=True,
        )
        for url in sorted(metadata["files"], reverse=True):
            filename, extension = get_filename_extension_from_url(url)
            match = self.distance_regex.search(filename)
            if match:
                filename = filename[: match.end()]
            resource = Resource(
                {
                    "name": f"{filename}{extension}".lower(),
                    "url": url,
                    "description": f"{self._resource_base_description}{metadata['popyear']}",
                    "format": metadata["data_format"],
                }
            )
            resource.set_date_data_updated(date)
            dataset.add_update_resource(resource)
