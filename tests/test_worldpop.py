#!/usr/bin/python
"""
Unit tests for worldpop.

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from worldpop import (
    generate_datasets_and_showcases,
    get_countriesdata,
    get_indicators_metadata,
)


class TestWorldPop:
    indicators_metadata = [
        {
            "alias": "pop",
            "name": "Population",
            "title": "Population",
            "desc": "WorldPop produces different types of gridded population count datasets...",
        },
        {
            "alias": "births",
            "name": "Births",
            "title": "Births",
            "desc": "The health and survival of women and their new-born babies in low income countries is a key public health priority...",
        },
        {
            "alias": "pregnancies",
            "name": "Pregnancies",
            "title": "Pregnancies",
            "desc": "The health and survival of women and their new-born babies in low income countries is a key public health priority...",
        },
        {
            "alias": "age_structures",
            "name": "Age and sex structures",
            "title": "Age and sex structures",
            "desc": "Age and sex structures: WorldPop produces different types of gridded population count datasets...",
        },
    ]
    countriesdata = {
        "AUS": {
            "pop": {
                "wpgp": ["http://papa/getJSON/pop/wpgp?iso3=AUS"],
                "wpgpunadj": ["http://papa/getJSON/pop/wpgpunadj?iso3=AUS"],
            }
        },
        "BRA": {
            "pop": {
                "wpgp": ["http://papa/getJSON/pop/wpgp?iso3=BRA"],
                "wpgpunadj": ["http://papa/getJSON/pop/wpgpunadj?iso3=BRA"],
            }
        },
        "CAN": {
            "pop": {
                "wpgp": ["http://papa/getJSON/pop/wpgp?iso3=CAN"],
                "wpgpunadj": ["http://papa/getJSON/pop/wpgpunadj?iso3=CAN"],
            }
        },
        "RUS": {
            "pop": {
                "wpgp": ["http://papa/getJSON/pop/wpgp?iso3=RUS"],
                "wpgpunadj": ["http://papa/getJSON/pop/wpgpunadj?iso3=RUS"],
            }
        },
        "World": {
            "pop": {
                "wpgp1km": [
                    "http://papa/getJSON/pop/wpgp1km?id=24776",
                    "http://papa/getJSON/pop/wpgp1km?id=24777",
                ]
            }
        },
        "ZWE": {
            "pop": {
                "wpgp": ["http://papa/getJSON/pop/wpgp?iso3=ZWE"],
                "wpgpunadj": ["http://papa/getJSON/pop/wpgpunadj?iso3=ZWE"],
            }
        },
    }
    wpgpdata = [
        {"id": "1325", "iso3": "AUS"},
        {"id": "1326", "iso3": "RUS"},
        {"id": "1327", "iso3": "BRA"},
        {"id": "1328", "iso3": "CAN"},
        {"id": "1482", "iso3": "ZWE"},
    ]
    wpgpunadjdata = [
        {"id": "13251", "iso3": "AUS"},
        {"id": "13261", "iso3": "RUS"},
        {"id": "13271", "iso3": "BRA"},
        {"id": "13281", "iso3": "CAN"},
        {"id": "14821", "iso3": "ZWE"},
    ]
    metadata = [
        {
            "id": "1482",
            "title": "The spatial distribution of population in 2000, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2000",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/1482/zwe_ppp_wpgp_2000_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=1482",
        },
        {
            "id": "1731",
            "title": "The spatial distribution of population in 2001, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2001",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/1731/zwe_ppp_wpgp_2001_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=1731",
        },
        {
            "id": "3474",
            "title": "The spatial distribution of population in 2008, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2008",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/3474/zwe_ppp_wpgp_2008_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=3474",
        },
        {
            "id": "4711",
            "title": "The spatial distribution of population in 2013, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2013",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/4711/zwe_ppp_wpgp_2013_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=4711",
        },
        {
            "id": "6205",
            "title": "The spatial distribution of population in 2019, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2019",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/6205/zwe_ppp_wpgp_2019_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=6205",
        },
        {
            "id": "6454",
            "title": "The spatial distribution of population in 2020, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell.",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2020",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=6454",
        },
    ]
    metadataunadj = [
        {
            "id": "14821",
            "title": "The spatial distribution of population in 2000 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2000",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2000/ZWE/zwe_ppp_2000_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/1482/zwe_ppp_wpgp_2000_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=14821",
        },
        {
            "id": "17311",
            "title": "The spatial distribution of population in 2001 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2001",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/1731/zwe_ppp_wpgp_2001_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=17311",
        },
        {
            "id": "34741",
            "title": "The spatial distribution of population in 2008 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2008",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/3474/zwe_ppp_wpgp_2008_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=34741",
        },
        {
            "id": "47111",
            "title": "The spatial distribution of population in 2013 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2013",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/4711/zwe_ppp_wpgp_2013_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=47111",
        },
        {
            "id": "62051",
            "title": "The spatial distribution of population in 2019 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2019",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/6205/zwe_ppp_wpgp_2019_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=62051",
        },
        {
            "id": "64541",
            "title": "The spatial distribution of population in 2020 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
            "desc": "Estimated total number of people per grid-cell. UNAdj",
            "doi": "10.5258/SOTON/WP00645",
            "date": "2018-11-01",
            "popyear": "2020",
            "citation": "WorldPop",
            "data_file": "GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif",
            "archive": "N",
            "public": "Y",
            "source": "WorldPop, University of Southampton, UK",
            "data_format": "geotiff",
            "author_email": "wp@worldpop.uk",
            "author_name": "WorldPop",
            "maintainer_name": "WorldPop",
            "maintainer_email": "wp@worldpop.uk",
            "project": "Population",
            "category": "Individual countries 2000-2020 UN adjusted ( 100m resolution )",
            "gtype": "Population",
            "continent": "Africa",
            "country": "Zimbabwe",
            "iso3": "ZWE",
            "files": [
                "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020_UNadj.tif"
            ],
            "url_img": "https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image_UNadj.png",
            "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
            "license": "https://www.worldpop.org/data/licence.txt",
            "url_summary": "https://www.worldpop.org/geodata/summary?id=64541",
        },
    ]
    wpgp1kmdata = [{"id": "24776"}, {"id": "24777"}]
    metadata_24777 = {
        "id": "24777",
        "title": "The spatial distribution of population in 2020",
        "desc": "Estimated total number of people per grid-cell...\r\n",
        "doi": "10.5258/SOTON/WP00647",
        "date": "0018-02-01",
        "popyear": "2020",
        "citation": "WorldPop...\r\n",
        "data_file": "GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif",
        "file_img": "world_ppp_wpgp_2020_Image.png",
        "archive": "N",
        "public": "Y",
        "source": "WorldPop, University of Southampton, UK",
        "data_format": "tiff",
        "author_email": "wp@worldpop.uk",
        "author_name": "WorldPop",
        "maintainer_name": "WorldPop",
        "maintainer_email": "wp@worldpop.uk",
        "project": "Population",
        "category": "Global mosaics 2000-2020",
        "gtype": "Population",
        "continent": "World",
        "country": None,
        "iso3": None,
        "files": [
            "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif"
        ],
        "url_img": "",
        "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
        "license": "https://www.worldpop.org/data/licence.txt",
        "url_summary": "https://www.worldpop.org/geodata/summary?id=24777",
    }
    metadata_24776 = {
        "id": "24776",
        "title": "The spatial distribution of population in 2019",
        "desc": "Estimated total number of people per grid-cell...\r\n",
        "doi": "10.5258/SOTON/WP00647",
        "date": "2018-11-01",
        "popyear": "2019",
        "citation": "WorldPop...\r\n",
        "data_file": "GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif",
        "file_img": "world_ppp_wpgp_2019_Image.png",
        "archive": "N",
        "public": "Y",
        "source": "WorldPop, University of Southampton, UK",
        "data_format": "tiff",
        "author_email": "wp@worldpop.uk",
        "author_name": "WorldPop",
        "maintainer_name": "WorldPop",
        "maintainer_email": "wp@worldpop.uk",
        "project": "Population",
        "category": "Global mosaics 2000-2020",
        "gtype": "Population",
        "continent": "World",
        "country": None,
        "iso3": None,
        "files": [
            "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif"
        ],
        "url_img": "https://www.worldpop.org/tabs/gdata/img/24776/world_ppp_wpgp_2019_Image.png",
        "organisation": "WorldPop, University of Southampton, UK, www.worldpop.org",
        "license": "https://www.worldpop.org/data/licence.txt",
        "url_summary": "https://www.worldpop.org/geodata/summary?id=24776",
    }

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [{"name": "zwe", "title": "Zimbabwe"}, {"name": "world", "title": "World"}]
        )
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            "tags": [{"name": "population"}, {"name": "geodata"}],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        class Download:
            url = None

            @classmethod
            def download(cls, url):
                cls.url = url

            @classmethod
            def get_json(cls):
                if cls.url == "http://lala/getJSON/":
                    return {"data": TestWorldPop.indicators_metadata}
                elif cls.url == "http://papa/getJSON/pop/wpgp":
                    return {"data": TestWorldPop.wpgpdata}
                elif cls.url == "http://papa/getJSON/pop/wpgpunadj":
                    return {"data": TestWorldPop.wpgpunadjdata}
                elif cls.url == "http://papa/getJSON/pop/wpgp1km":
                    return {"data": TestWorldPop.wpgp1kmdata}
                elif cls.url == "http://papa/getJSON/pop/wpgp?iso3=ZWE":
                    return {"data": TestWorldPop.metadata}
                elif cls.url == "http://papa/getJSON/pop/wpgpunadj?iso3=ZWE":
                    return {"data": TestWorldPop.metadataunadj}
                elif cls.url == "http://papa/getJSON/pop/wpgp1km?id=24776":
                    return {"data": TestWorldPop.metadata_24776}
                elif cls.url == "http://papa/getJSON/pop/wpgp1km?id=24777":
                    return {"data": TestWorldPop.metadata_24777}

            @staticmethod
            def get_text():
                return (
                    "The WorldPop project aims to provide an open access archive of spatial "
                    "demographic datasets ... at creativecommons.org."
                )

        return Download()

    def test_get_indicators_metadata(self, configuration, downloader):
        indicators = configuration["indicators"]
        indicators_metadata = get_indicators_metadata(
            "http://lala/getJSON/", downloader, indicators
        )
        assert "pop" in indicators_metadata.keys()
        assert sorted(
            list(indicators_metadata.values()), key=lambda k: k["alias"]
        ) == sorted(TestWorldPop.indicators_metadata, key=lambda k: k["alias"])

    def test_get_countriesdata(self, configuration, downloader):
        indicators = configuration["indicators"]
        cutdownindicators = {"pop": indicators["pop"]}
        countriesdata, countries = get_countriesdata(
            "http://papa/getJSON/", downloader, cutdownindicators
        )
        assert countriesdata == TestWorldPop.countriesdata
        assert countries == [
            {"iso3": "AUS"},
            {"iso3": "BRA"},
            {"iso3": "CAN"},
            {"iso3": "RUS"},
            {"iso3": "ZWE"},
            {"iso3": "World"},
        ]

    def test_generate_datasets_and_showcases(self, configuration, downloader):
        indicators_metadata = {"pop": TestWorldPop.indicators_metadata[0]}
        countryiso = "World"
        countrydata = TestWorldPop.countriesdata[countryiso]
        datasets, showcases = generate_datasets_and_showcases(
            downloader, countryiso, indicators_metadata, countrydata
        )
        dataset = datasets[0]
        assert dataset == {
            "name": "worldpop-population-for-world",
            "title": "World - Population",
            "notes": "WorldPop produces different types of gridded population count datasets...\r\nData for earlier dates is available directly from WorldPop.\r\nWorldPop...\r\n",
            "methodology": "Other",
            "methodology_other": "Estimated total number of people per grid-cell...\r\n",
            "dataset_source": "WorldPop, University of Southampton, UK",
            "license_id": "hdx-other",
            "license_other": "The WorldPop project aims to provide an open access archive of spatial demographic datasets ... at creativecommons.org.",
            "private": False,
            "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
            "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
            "data_update_frequency": "365",
            "subnational": "1",
            "groups": [{"name": "world"}],
            "tags": [
                {
                    "name": "population",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "geodata",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
            "dataset_date": "[2019-01-01T00:00:00 TO 2020-12-31T00:00:00]",
        }

        resources = dataset.get_resources()
        assert resources == [
            {
                "name": "ppp_2020_1km_Aggregated.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/0_Mosaicked/ppp_2020_1km_Aggregated.tif",
                "description": "The spatial distribution of population in 2020",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "ppp_2019_1km_Aggregated.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/0_Mosaicked/ppp_2019_1km_Aggregated.tif",
                "description": "The spatial distribution of population in 2019",
                "resource_type": "api",
                "url_type": "api",
            },
        ]

        showcase = next(iter(showcases.values()))[0]
        assert showcase == {
            "name": "worldpop-population-for-world-showcase",
            "title": "WorldPop World Population Summary Page",
            "notes": "Summary for Global mosaics 2000-2020 - World",
            "url": "https://www.worldpop.org/geodata/summary?id=24777",
            "image_url": "https://www.worldpop.org/tabs/gdata/img/24776/world_ppp_wpgp_2019_Image.png",
            "tags": [
                {
                    "name": "population",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "geodata",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
        }

        countryiso = "ZWE"
        countrydata = TestWorldPop.countriesdata[countryiso]
        datasets, showcases = generate_datasets_and_showcases(
            downloader, countryiso, indicators_metadata, countrydata
        )
        dataset = datasets[0]
        assert dataset == {
            "name": "worldpop-population-for-zimbabwe",
            "title": "Zimbabwe - Population",
            "notes": "WorldPop produces different types of gridded population count datasets...\r\nData for earlier dates is available directly from WorldPop.\r\nWorldPop",
            "methodology": "Other",
            "methodology_other": "Estimated total number of people per grid-cell. UNAdj",
            "dataset_source": "WorldPop, University of Southampton, UK",
            "license_id": "hdx-other",
            "license_other": "The WorldPop project aims to provide an open access archive of spatial demographic datasets ... at creativecommons.org.",
            "private": False,
            "maintainer": "37023db4-a571-4f28-8d1f-15f0353586af",
            "owner_org": "3f077dff-1d05-484d-a7c2-4cb620f22689",
            "data_update_frequency": "365",
            "subnational": "1",
            "groups": [{"name": "zwe"}],
            "tags": [
                {
                    "name": "population",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "geodata",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
            "dataset_date": "[2000-01-01T00:00:00 TO 2020-12-31T00:00:00]",
        }

        resources = dataset.get_resources()
        assert resources == [
            {
                "name": "zwe_ppp_2020.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020.tif",
                "description": "The spatial distribution of population in 2020, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2020_UNadj.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/ZWE/zwe_ppp_2020_UNadj.tif",
                "description": "The spatial distribution of population in 2020 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2019.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019.tif",
                "description": "The spatial distribution of population in 2019, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2019_UNadj.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2019/ZWE/zwe_ppp_2019_UNadj.tif",
                "description": "The spatial distribution of population in 2019 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2013.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013.tif",
                "description": "The spatial distribution of population in 2013, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2013_UNadj.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2013/ZWE/zwe_ppp_2013_UNadj.tif",
                "description": "The spatial distribution of population in 2013 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2008.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008.tif",
                "description": "The spatial distribution of population in 2008, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2008_UNadj.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2008/ZWE/zwe_ppp_2008_UNadj.tif",
                "description": "The spatial distribution of population in 2008 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2001.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001.tif",
                "description": "The spatial distribution of population in 2001, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "zwe_ppp_2001_UNadj.tif",
                "format": "geotiff",
                "url": "ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2001/ZWE/zwe_ppp_2001_UNadj.tif",
                "description": "The spatial distribution of population in 2001 with country total adjusted to match the corresponding UNPD estimate, Zimbabwe",
                "resource_type": "api",
                "url_type": "api",
            }
        ]

        showcase = next(iter(showcases.values()))[0]
        assert showcase == {
            "name": "worldpop-population-for-zimbabwe-showcase",
            "title": "WorldPop Zimbabwe Population Summary Page",
            "notes": "Summary for Individual countries 2000-2020 ( 100m resolution ) - Zimbabwe",
            "url": "https://www.worldpop.org/geodata/summary?id=6454",
            "image_url": "https://www.worldpop.org/tabs/gdata/img/6454/zwe_ppp_wpgp_2020_Image.png",
            "tags": [
                {
                    "name": "population",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
                {
                    "name": "geodata",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                },
            ],
        }
