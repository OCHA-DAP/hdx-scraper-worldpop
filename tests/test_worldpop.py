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
                {"name": "xkx", "title": "Kosovo"},
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
                assert len(countries_data) == 242
                assert (
                    countries_data["AFG"]["pop"]
                    == "https://hub.worldpop.org/rest/data/pop/G2_CN_POP_R25A_100m?iso3=AFG"
                )
                assert (
                    countries_data["AFG"]["age_structures"]
                    == "https://hub.worldpop.org/rest/data/age_structures/G2_CN_Age_R25A_3a_z?iso3=AFG"
                )
                assert (
                    countries_data["XKX"]["pop"]
                    == "https://hub.worldpop.org/rest/data/pop/G2_CN_POP_R25A_100m?iso3=KOS"
                )
                assert (
                    countries_data["XKX"]["age_structures"]
                    == "https://hub.worldpop.org/rest/data/age_structures/G2_CN_Age_R25A_3a_z?iso3=KOS"
                )
                datasets, showcases = worldpop.generate_datasets_and_showcases("AFG")
                assert len(datasets) == 2
                dataset = datasets[0]
                assert dataset == {
                    "caveats": "Disclaimer:The dataset currently represents an alpha version (R2025A) product and may change over the coming year as improvements are made.  \n  \nData for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. 2025 Constrained estimates of 2015-2030 total number of people per grid square at a resolution of 3 arc (approximately 100m at the equator) R2025A version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00839",
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-01-01T00:00:00]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-population-counts-2015-2030-afg",
                    "notes": "Estimates, total number of people per grid-cell. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are number of people per pixel. The mapping approach is Random Forest-based dasymetric redistribution.  \n  \nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf)  \n  \nPlease note that these data represent 2025 Alpha release versions, constructed in September 2025",
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
                assert dataset == {
                    "caveats": "Data for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. 2025. Estimates of 2015-2030 total number of people per grid square broken down by gender and age groupings at a resolution of 3 arc (approximately 100m at the equator) R2025A version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00841",
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-01-01T00:00:00]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-age-and-gender-structures-2015-2030-afg",
                    "notes": "Estimates of total number of people per grid square broken down by gender and age groupings (including 0-1 and by 5-year up to 90+) for Afghanistan, R2025A version v1. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are estimated number of male, female or both in each age group per grid square.&nbsp;  \n  \nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf)  \n  \nPlease note that these data represent 2025 Alpha release versions, constructed in September 2025  \n  \n**File Descriptions:**  \n  \n_{iso}\xa0{gender}\xa0{age group}\xa0{year}\xa0{type}\xa0{resolution}.tif_  \n  \n_iso_  \n  \nThree-letter country code  \n  \n_gender_  \n  \nm = male, f= female, t = both genders  \n  \n_age group_  \n  \n*   00 = age group 0 to 12 months  \n*   01 = age group 1 to 4 years  \n*   05 = age group 5 to 9 years  \n*   90 = age 90 years and over  \n  \n_year_  \n  \nYear that the population represents  \n  \n_type_  \n  \nCN = [Constrained](https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained/)  \n  \n_resolution_  \n  \nResolution of the data e.q. 100m = 3 arc (approximately 100m at the equator)",
                    "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "demographics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan - Age and Gender Structures (2015-2030)",
                }
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
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/72287/afg_population_2024_CN_100m_R2025A_v1_Image.png",
                    "name": "worldpop-population-counts-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Individual countries "
                    "2015-2030 ( 100m resolution ) R2025A v1",
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
                    "url": "https://hub.worldpop.org/geodata/summary?id=72287",
                }
                assert showcases[1] == {
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/103295/afg_agesex_CN_100m_R2024B_v1_Image.png",
                    "name": "worldpop-age-and-gender-structures-2015-2030-afg-showcase",
                    "notes": "Afghanistan WorldPop summary: Individual countries "
                    "2015-2030 ( 100m resolution ) R2025A v1-z",
                    "tags": [
                        {
                            "name": "baseline population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "demographics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "2025 Age and gender structures",
                    "url": "https://hub.worldpop.org/geodata/summary?id=103295",
                }

                datasets, showcases = worldpop.generate_datasets_and_showcases("XKX")
                assert len(datasets) == 2
                dataset = datasets[0]
                assert dataset == {
                    "caveats": "Disclaimer:The dataset currently represents an alpha version (R2025A) product and may change over the coming year as improvements are made.  \n  \nData for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. 2025 Constrained estimates of 2015-2030 total number of people per grid square at a resolution of 3 arc (approximately 100m at the equator) R2025A version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00839",
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-01-01T00:00:00]",
                    "groups": [{"name": "xkx"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-population-counts-2015-2030-xkx",
                    "notes": "Estimates, total number of people per grid-cell. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are number of people per pixel. The mapping approach is Random Forest-based dasymetric redistribution.  \n  \nMore information can be found in the [Release Statement](https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf)  \n  \nPlease note that these data represent 2025 Alpha release versions, constructed in September 2025",
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
                    "title": "Kosovo - Spatial Distribution of Population (2015-2030)",
                }
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
                resources = dataset.get_resources()
                assert resources[21] == {
                    "description": "Individual age and gender structures (1km resolution) for 2025",
                    "format": "Geotiff",
                    "last_modified": "2025-09-01T00:00:00.000000",
                    "name": "xkx_agegender_structures_2025_cn_1km.zip",
                    "url": "https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2025/XKX/v1/1km_ua/xkx_agesex_structures_2025_CN_1km_R2025A_UA_v1.zip",
                }
