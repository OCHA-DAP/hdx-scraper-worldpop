#!/usr/bin/python
"""
Unit tests for worldpop.

"""

from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.worldpop.pipeline import Pipeline
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_json
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent


class TestWorldPop:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=script_dir_plus_file(
                join("config", "project_configuration.yaml"), Pipeline
            ),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "xkx", "title": "Kosovo"},
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "baseline population",
                    "census",
                    "demographics",
                    "gender and age disaggregated data-gadd",
                    "geodata",
                    "population",
                )
            ],
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def metadata(self, fixtures_dir):
        return load_json(join(fixtures_dir, "sheet_metadata.json"))

    def test_main(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        metadata,
    ):
        with temp_dir(
            "TestWorldPop",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader,
                    tempdir,
                    input_dir,
                    tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(retriever, retriever, configuration, metadata, 2025)
                countries_data, countries = pipeline.get_countriesdata()
                # VAT has total_population null so is skipped
                assert countries == [{"iso3": "AFG"}, {"iso3": "XKX"}]

                pop_metadata = metadata["pop"]
                pop_citation = (
                    "Bondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., "
                    "McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk "
                    "B., Brinkhoff T., Tatem A., Sorichetta A.. 2025 Constrained "
                    "estimates of 2015-2030 total number of people per grid square at "
                    "a resolution of 3 arc (approximately 100m at the equator) R2025A "
                    "version v1. Global Demographic Data Project - Funded by The Bill "
                    "and Melinda Gates Foundation (INV-045237). WorldPop - School of "
                    "Geography and Environmental Science, University of Southampton. "
                    "DOI:10.5258/SOTON/WP00839"
                )
                age_metadata = metadata["age_structures"]
                vocabulary_id = "b891512e-9516-4bf5-962a-7a289772a2a1"
                pop_tags = [
                    {"name": "baseline population", "vocabulary_id": vocabulary_id},
                    {"name": "census", "vocabulary_id": vocabulary_id},
                    {"name": "geodata", "vocabulary_id": vocabulary_id},
                    {"name": "population", "vocabulary_id": vocabulary_id},
                ]
                age_tags = [
                    {"name": "baseline population", "vocabulary_id": vocabulary_id},
                    {"name": "census", "vocabulary_id": vocabulary_id},
                    {"name": "demographics", "vocabulary_id": vocabulary_id},
                    {
                        "name": "gender and age disaggregated data-gadd",
                        "vocabulary_id": vocabulary_id,
                    },
                    {"name": "geodata", "vocabulary_id": vocabulary_id},
                    {"name": "population", "vocabulary_id": vocabulary_id},
                ]

                datasets, showcases = pipeline.generate_datasets_and_showcases("AFG")
                assert len(datasets) == 2
                dataset = datasets[0]
                assert dataset == {
                    "caveats": pop_metadata["Caveats"] + "\n\n" + pop_citation,
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-01-01T00:00:00]",
                    "dataset_source": "Multiple sources",
                    "groups": [{"name": "afg"}],
                    "license_id": "cc-by",
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "methodology_other": pop_metadata["Methodology"],
                    "name": "worldpop-population-counts-2015-2030-afg",
                    "notes": "Estimates of the total number of people per grid cell for Afghanistan for each year from 2015 to 2030, R2025A version v1. The files are Geotiffs at 100m and 1km resolution, in the WGS84 Geographic Coordinate System. The units are number of people per pixel.\n\nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf)\n\n**File Descriptions**\n\nEach resource is a Geotiff for one year and resolution, named:\n\n*iso*\\_*pop*\\_*year*\\_*type*\\_*resolution*.tif\n\n*iso*\n\nThree-letter country code\n\n*year*\n\nYear that the population represents\n\n*type*\n\nCN = [Constrained](https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained/)\n\n*resolution*\n\nResolution of the data: 100m is 3 arc, approximately 100m at the equator; 1km is 30 arc, approximately 1km at the equator",
                    "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
                    "subnational": "1",
                    "tags": pop_tags,
                    "title": "Afghanistan - Spatial Distribution of Population (2015-2030)",
                }
                resources = dataset.get_resources()
                assert len(resources) == 32
                assert resources[6] == {
                    "description": "Individual population counts (100m resolution) for 2018",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "afg_pop_2018_cn_100m.tif",
                    "url": "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2018/AFG/v1/100m/constrained/afg_pop_2018_CN_100m_R2025A_v1.tif",
                }
                assert resources[7] == {
                    "description": "Individual population counts (1km resolution) for 2018",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "afg_pop_2018_cn_1km.tif",
                    "url": "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2018/AFG/v1/1km_ua/constrained/afg_pop_2018_CN_1km_R2025A_UA_v1.tif",
                }

                dataset = datasets[1]
                assert (
                    dataset["name"]
                    == "worldpop-age-and-gender-structures-2015-2030-afg"
                )
                assert (
                    dataset["title"]
                    == "Afghanistan - Age and Gender Structures (2015-2030)"
                )
                notes = dataset["notes"]
                assert notes.startswith(
                    "Estimates of the number of people per grid cell by gender and "
                    "age group for Afghanistan for each year from 2015 to 2030, "
                    "R2025A version v1."
                )
                assert "**File Descriptions**" in notes
                assert (
                    "* 00 = age group 0 to 12 months\n* 01 = age group 1 to 4 years"
                    in notes
                )
                assert "100m and 1km resolution" in notes
                assert "for example afg_f_00_2030_CN_100m_R2025A_v1.tif" in notes
                assert "{" not in notes
                assert dataset["caveats"].startswith(age_metadata["Caveats"])
                assert dataset["caveats"].endswith("DOI:10.5258/SOTON/WP00841")
                assert dataset["tags"] == age_tags
                resources = dataset.get_resources()
                assert len(resources) == 32
                assert resources[20] == {
                    "description": "Individual age and gender structures (100m resolution) for 2025",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "afg_agegender_structures_2025_cn_100m.zip",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2025/AFG/v1/100m/afg_agesex_structures_2025_CN_100m_R2025A_v1.zip",
                }
                assert resources[21] == {
                    "description": "Individual age and gender structures (1km resolution) for 2025",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "afg_agegender_structures_2025_cn_1km.zip",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2025/AFG/v1/1km_ua/afg_agesex_structures_2025_CN_1km_R2025A_UA_v1.zip",
                }

                assert len(showcases) == 2
                assert showcases[0] == {
                    "image_url": "https://api.worldpop.org/v3/profiles/place.png?iso3=AFG",
                    "name": "worldpop-population-counts-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Individual countries "
                    "2015-2030 (100m resolution) R2025A v1",
                    "tags": pop_tags,
                    "title": "2025 Population Counts",
                    "url": "https://hub.worldpop.org/geodata/summary?id=72287",
                }
                assert showcases[1] == {
                    "image_url": "https://api.worldpop.org/v3/profiles/place.png?iso3=AFG",
                    "name": "worldpop-age-and-gender-structures-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Individual countries "
                    "2015-2030 (100m resolution) R2025A v1",
                    "tags": age_tags,
                    "title": "2025 Age and gender structures",
                    "url": "https://hub.worldpop.org/geodata/summary?id=103295",
                }

                datasets, showcases = pipeline.generate_datasets_and_showcases("XKX")
                assert len(datasets) == 2
                dataset = datasets[0]
                assert dataset["name"] == "worldpop-population-counts-2015-2030-xkx"
                assert (
                    dataset["title"]
                    == "Kosovo - Spatial Distribution of Population (2015-2030)"
                )
                assert dataset["groups"] == [{"name": "xkx"}]
                resources = dataset.get_resources()
                assert len(resources) == 32
                assert resources[6] == {
                    "description": "Individual population counts (100m resolution) for 2018",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "xkx_pop_2018_cn_100m.tif",
                    "url": "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2018/XKX/v1/100m/constrained/xkx_pop_2018_CN_100m_R2025A_v1.tif",
                }
                dataset = datasets[1]
                assert "for Kosovo for each year" in dataset["notes"]
                resources = dataset.get_resources()
                assert resources[21] == {
                    "description": "Individual age and gender structures (1km resolution) for 2025",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "xkx_agegender_structures_2025_cn_1km.zip",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2025/XKX/v1/1km_ua/xkx_agesex_structures_2025_CN_1km_R2025A_UA_v1.zip",
                }
                assert (
                    showcases[0]["image_url"]
                    == "https://api.worldpop.org/v3/profiles/place.png?iso3=XKX"
                )
                assert (
                    showcases[0]["url"]
                    == "https://hub.worldpop.org/geodata/summary?id=76143"
                )
                assert (
                    showcases[1]["url"]
                    == "https://hub.worldpop.org/geodata/summary?id=107151"
                )
