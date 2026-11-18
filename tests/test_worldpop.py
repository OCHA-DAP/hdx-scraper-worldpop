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
                    "caveats": pop_metadata["Caveats"],
                    "data_update_frequency": "365",
                    "dataset_date": "[2015-01-01T00:00:00 TO 2030-01-01T00:00:00]",
                    "dataset_source": "Multiple sources",
                    "groups": [{"name": "afg"}],
                    "license_other": "[WorldPop licence information](https://hub.worldpop.org/data/licence.txt)",
                    "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
                    "methodology_other": pop_metadata["Methodology"],
                    "name": "worldpop-population-counts-2015-2030-afg",
                    "notes": pop_metadata["Dataset Description"],
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
                assert dataset["notes"] == age_metadata["Dataset Description"].format(
                    countryname="Afghanistan"
                )
                assert (
                    "(including 0-1 and by 5-year up to 90+) for Afghanistan, R2025A"
                    in dataset["notes"]
                )
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
                assert "for Kosovo, R2025A" in dataset["notes"]
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
