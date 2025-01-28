import logging
import re

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_filename_extension_from_url

logger = logging.getLogger(__name__)


class AliasData:
    distance_regex = re.compile(r"_\d*m")

    def __init__(
        self, retriever, countryiso3, indicator_metadata, countrydata
    ):
        self._retriever = retriever
        self._countryiso3 = countryiso3
        self._indicator_metadata = indicator_metadata
        self._countrydata = countrydata
        self._allmetadata = {}
        self._countryname = None
        self._showcases = {}

    def get_showcases(self):
        return self._showcases

    def get_allmetadata_for_country(self):
        for subalias, urls in self._countrydata.items():
            allmetadata_subalias = self._allmetadata.get(subalias, [])
            for url in urls:
                json = self._retriever.download_json(url)
                data = json["data"]
                if isinstance(data, list):
                    allmetadata_subalias.extend(data)
                else:
                    allmetadata_subalias.append(data)
            self._allmetadata[subalias] = allmetadata_subalias
        return self._allmetadata

    def get_tags(self):
        return [self._indicator_metadata["name"].lower(), "geodata"]

    def set_country_name(self):
        if self._countryiso3 == "World":
            self._countryname = self._countryiso3
        else:
            self._countryname = Country.get_country_name_from_iso3(
                self._countryiso3
            )
            if not self._countryname:
                logger.exception(f"ISO3 {self._countryiso3} not recognised!")
                return False
        return True

    def get_caveats(self):
        allmetadatavalues = list(self._allmetadata.values())
        lastmetadata = allmetadatavalues[0][-1]
        citation = lastmetadata["citation"]
        return f"Data for earlier dates is available directly from WorldPop  \n  \n{citation}"

    def generate_dataset(self, metadata):
        dataset = Dataset()
        try:
            dataset.add_other_location(self._countryiso3)
        except HDXError as e:
            logger.exception(f"{self._countryiso3} has a problem! {e}")
            return None, None
        project = metadata["project"]
        category = metadata["category"]
        title = metadata["title"]
        year = int(metadata["popyear"])
        estimate_type, _ = category.split(maxsplit=1)
        if "-" in category:
            start, end = category.split("-")
            year_range = f"{start[-4:]}-{end[:4]}"
            category_shorter = f"{estimate_type.lower()} {year_range}"
            title = f"{title} for {year} ({category_shorter})"
        else:
            category_shorter = estimate_type.lower()
            title = f"{title} ({estimate_type})"
        base_name = slugify(
            f"worldpop-{project}-{category_shorter}-{self._countryiso3}"
        )
        logger.info(f"Creating dataset: {title}")
        dataset["name"] = f"{base_name}-{year}"
        dataset["title"] = title
        dataset["notes"] = metadata["desc"]
        dataset["caveats"] = self.get_caveats()
        dataset.set_maintainer("37023db4-a571-4f28-8d1f-15f0353586af")
        dataset.set_organization("3f077dff-1d05-484d-a7c2-4cb620f22689")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(True)
        dataset.set_time_period_year_range(year)
        dataset.add_tags(self.get_tags())

        bracketed_text = category.split("(", 1)[1].split(")")[0].strip()
        resource_data = {
            "description": f"{estimate_type} {project.lower()} ({bracketed_text}) for {year}",
            "format": metadata["data_format"],
        }

        url_img = metadata["url_img"]
        url_summary = metadata["url_summary"]
        showcase = Showcase(
            {
                "name": base_name,
                "title": f"WorldPop {self._countryname} {project} Summary Page",
                "notes": f"Summary for {category} - {self._countryname}",
                "url": url_summary,
                "image_url": url_img,
            }
        )
        showcase.add_tags(self.get_tags())
        self._showcases[base_name] = showcase
        return dataset, resource_data

    def generate_datasets(self):
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
        datasets = []
        for subalias, subalias_metadata in self._allmetadata.items():
            for metadata in subalias_metadata:
                if metadata["public"].lower() != "y":
                    continue
                dataset, resource_data = self.generate_dataset(metadata)
                if not dataset:
                    continue
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
                    resource = Resource(resource_data)
                    resource["name"] = f"{filename}{extension}".lower()
                    resource["url"] = url
                    resource.set_date_data_updated(date)
                    dataset.add_update_resource(resource)
                if len(dataset.get_resources()) == 0:
                    logger.error(f"{dataset['title']} has no data!")
                    continue
                datasets.append(dataset)
        return datasets
