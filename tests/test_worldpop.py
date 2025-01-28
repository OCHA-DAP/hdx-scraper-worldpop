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
                worldpop = Pipeline(retriever, configuration)
                indicators_metadata = worldpop.get_indicators_metadata()
                assert len(indicators_metadata) == 2
                assert (
                    indicators_metadata["age_structures"]["name"]
                    == "Age and sex structures"
                )
                countries_data, countries = worldpop.get_countriesdata()
                assert len(countries_data) == 238
                assert countries_data["AFG"]["pop"]["G2_CN_POP_2024_100m"] == [
                    "https://hub.worldpop.org/rest/data/pop/G2_CN_POP_2024_100m?iso3=AFG"
                ]
                datasets, showcases = (
                    worldpop.generate_all_datasets_and_showcases("AFG")
                )
                assert len(datasets) == 68
                assert datasets[1] == {
                    "caveats": "Data for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. Constrained estimates of 2024 total number of people per grid square at a resolution of 3 arc (approximately 100m at the equator) R2024A version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00801",
                    "data_update_frequency": "365",
                    "dataset_date": "[2024-01-01T00:00:00 TO 2024-12-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-population-counts-unconstrained-afg-2024",
                    "notes": "Unconstrained estimates of 2024, total number of people per grid-cell. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are number of people per pixel. The mapping approach is Random Forest-based dasymetric redistribution.",
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
                    "title": "The spatial distribution of population in 2024, Afghanistan (Unconstrained)",
                }
                assert datasets[51] == {
                    "caveats": "Data for earlier dates is available directly from WorldPop  \n  \nBondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., Tatem A., Sorichetta A.. Constrained estimates of 2024 total number of people per grid square broken down by gender and age groupings at a resolution of 3 arc (approximately 100m at the equator) R2024A version v1. Global Demographic Data Project - Funded by The Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00803",
                    "data_update_frequency": "365",
                    "dataset_date": "[2030-01-01T00:00:00 TO 2030-12-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "name": "worldpop-age-and-sex-structures-constrained-2015-2030-afg-2030",
                    "notes": "Constrained estimates of total number of people per grid square broken down by gender and age groupings (including 0-1 and by 5-year up to 90+) in 2030 for Afghanistan, version v1. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are estimated number of male, female or both in each age group per grid square.&nbsp;File Descriptions:{iso}_{gender}_{age group}_{year}_{type}_{resolution}_{release}_{version}.tifisoThree-letter country codegenderm = male, f= female, t = both gendersage group00 = age group 0 to 12 months01 = age group 1 to 4 years05 = age group 5 to 9 years90 = age 90 years and overyearYear that the population representstypeCN = Constrained , UC= Unconstrained&nbsp;resolutionResolution of the data e.q. 100m = 3 arc (approximately 100m at the equator)releaseReleaseversionVersionExample: afg_f_00_2030_UC_100m_R2024B_v1.tif &ndash; this dataset represents constrained estimates of total number of females of age group 0 to 12 months per grid square in Afghanistan for 2030 at 100m resolution, version R2024B v1.&nbsp;Disclaimer:The dataset currently represents a beta version (R2024B) product and may change over the coming year as improvements are made.",
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
                    "title": "Afghanistan - Age and Sex Structures for 2030 (constrained 2015-2030)",
                }

                assert len(showcases) == 8
                assert showcases[
                    "worldpop-population-counts-constrained-2015-2030-afg"
                ] == {
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/52040/afg_pop_2019_CN_100m_R2024B_v1_Image.png",
                    "name": "worldpop-population-counts-constrained-2015-2030-afg",
                    "notes": "Summary for Constrained individual countries 2015-2030 ( 100m resolution ) R2024B v1 - Afghanistan",
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
                    "title": "WorldPop Afghanistan Population Counts Summary Page",
                    "url": "https://hub.worldpop.org/geodata/summary?id=52040",
                }
                assert showcases[
                    "worldpop-age-and-sex-structures-constrained-afg"
                ] == {
                    "image_url": "https://hub.worldpop.org/tabs/gdata/img/51398/afg_t_00_2024_CN_100m_R2024A_v1_Image.png",
                    "name": "worldpop-age-and-sex-structures-constrained-afg",
                    "notes": "Summary for Constrained individual countries 2024 ( 100m resolution ) - Afghanistan",
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
                    "title": "WorldPop Afghanistan Age and sex structures Summary Page",
                    "url": "https://hub.worldpop.org/geodata/summary?id=51398",
                }
