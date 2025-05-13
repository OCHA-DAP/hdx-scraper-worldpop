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
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "baseline population",
                    "demographics",
                    "geodata",
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

    def test_main(
        self,
        configuration,
        fixtures_dir,
        input_dir,
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
                worldpop = Pipeline(retriever, configuration, 2025)
                indicators_metadata = worldpop.get_indicators_metadata()
                assert len(indicators_metadata) == 2
                assert indicators_metadata["pop"]["name"] == "Population Counts"
                assert (
                    indicators_metadata["age_structures"]["name"]
                    == "Age and sex structures"
                )
                countries_data, countries = worldpop.get_countriesdata()
                assert len(countries_data) == 26
                assert (
                    countries_data["AFG"]["pop"]
                    == "https://hub.worldpop.org/rest/data/pop/G2_CN_POP_R24B_100m?iso3=AFG"
                )
                assert (
                    countries_data["AFG"]["age_structures"]
                    == "https://hub.worldpop.org/rest/data/age_structures/G2_CN_Age_R24B_3a_z?iso3=AFG"
                )
                datasets, showcases = worldpop.generate_datasets_and_showcases("AFG")
                assert len(datasets) == 2
                dataset = datasets[0]
                assert dataset == {
                    "caveats": "Disclaimer:The dataset currently represents a beta version (R2024B) product and may change over the coming year as improvements are made.  \n  \nData for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. Constrained estimates of 2015-2030 total number of people per grid square at a resolution of 3 arc (approximately 100m at the equator) R2024B version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00803",
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-12-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-population-counts-2015-2030-afg",
                    "notes": "Constrained estimates, total number of people per grid-cell. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are number of people per pixel. The mapping approach is Random Forest-based dasymetric redistribution.&nbsp;  \n  \nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2024B/doc/Global2_Release_Statement_R2024B_v1.pdf)  \n  \nThe difference between constrained and unconstrained is explained on this page: https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained",
                    "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan - Spatial Distribution of Population (2015-2030)",
                }
                resources = dataset.get_resources()
                assert len(resources) == 32
                assert resources[6] == {
                    "description": "Constrained population counts (100m resolution) for 2018",
                    "format": "geotiff",
                    "last_modified": "2024-12-01T00:00:00.000000",
                    "name": "afg_pop_2018_cn_100m.tif",
                    "resource_type": "api",
                    "url": "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2018/AFG/v1/100m/constrained/afg_pop_2018_CN_100m_R2024B_v1.tif",
                    "url_type": "api",
                }
                assert resources[7] == {
                    "description": "Constrained population counts (1km resolution) for 2018",
                    "format": "geotiff",
                    "last_modified": "2024-12-01T00:00:00.000000",
                    "name": "afg_pop_2018_cn_1km.tif",
                    "resource_type": "api",
                    "url": "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2018/AFG/v1/1km_ua/constrained/afg_pop_2018_CN_1km_R2024B_UA_v1.tif",
                    "url_type": "api",
                }

                dataset = datasets[1]
                assert dataset == {
                    "caveats": "Disclaimer:The dataset currently represents a beta version (R2024B) product and may change over the coming year as improvements are made.  \n  \nData for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. Constrained estimates of 2015-2030 total number of people per grid square broken down by gender and age groupings at a resolution of 3 arc (approximately 100m at the equator) R2024B version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00805",
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-12-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-age-and-sex-structures-2015-2030-afg",
                    "notes": "Constrained estimates of total number of people per grid square broken down by gender and age groupings (including 0-1 and by 5-year up to 90+) for Afghanistan, version v1. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are estimated number of male, female or both in each age group per grid square.&nbsp;  \n  \nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2024B/doc/Global2_Release_Statement_R2024B_v1.pdf)  \n  \nThe difference between constrained and unconstrained is explained on this page: https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained  \n  \n**File Descriptions:**  \n  \n_{iso}\xa0{gender}\xa0{age group}\xa0{year}\xa0{type}\xa0{resolution}.tif_  \n  \n_iso_  \n  \nThree-letter country code  \n  \n_gender_  \n  \nm = male, f= female, t = both genders  \n  \n_age group_  \n  \n*   00 = age group 0 to 12 months  \n*   01 = age group 1 to 4 years  \n*   05 = age group 5 to 9 years  \n*   90 = age 90 years and over  \n  \n_year_  \n  \nYear that the population represents  \n  \n_type_  \n  \nCN = [Constrained](https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained/) , UC= [Unconstrained](https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained/)  \n  \n_resolution_  \n  \nResolution of the data e.q. 100m = 3 arc (approximately 100m at the equator)",
                    "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "demographics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan - Age and Sex Structures (2015-2030)",
                }
                resources = dataset.get_resources()
                assert len(resources) == 32
                assert resources[20] == {
                    "description": "Constrained age and sex structures (100m resolution) for 2025",
                    "format": "geotiff",
                    "last_modified": "2024-12-01T00:00:00.000000",
                    "name": "afg_agesex_structures_2025_cn_100m.zip",
                    "resource_type": "api",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2024B/2025/AFG/v1/100m/afg_agesex_structures_2025_CN_100m_R2024B_v1.zip",
                    "url_type": "api",
                }
                assert resources[21] == {
                    "description": "Constrained age and sex structures (1km resolution) for 2025",
                    "format": "geotiff",
                    "last_modified": "2024-12-01T00:00:00.000000",
                    "name": "afg_agesex_structures_2025_cn_1km.zip",
                    "resource_type": "api",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2024B/2025/AFG/v1/1km_ua/afg_agesex_structures_2025_CN_1km_R2024B_UA_v1.zip",
                    "url_type": "api",
                }

                assert len(showcases) == 2
                assert showcases[0] == {
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/52035/afg_pop_2019_CN_100m_R2024B_v1_Image.png",
                    "name": "worldpop-population-counts-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Constrained individual countries "
                    "2015-2030 ( 100m resolution ) R2024B v1",
                    "tags": [
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "2025 Population Counts",
                    "url": "https://hub.worldpop.org/geodata/summary?id=52035",
                }
                assert showcases[1] == {
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/53239/afg_agesex_CN_100m_R2024B_v1_Image.png",
                    "name": "worldpop-age-and-sex-structures-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Constrained individual countries "
                    "2015-2030 ( 100m resolution ) R2024B v1-z",
                    "tags": [
                        {
                            "name": "demographics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "2025 Age and sex structures",
                    "url": "https://hub.worldpop.org/geodata/summary?id=53239",
                }
