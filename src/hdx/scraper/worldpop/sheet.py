import json
import logging
from typing import Dict

import gspread
from hdx.api.configuration import Configuration
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Sheet:
    def __init__(self, gsheet_auth: str, configuration: Configuration):
        info = json.loads(gsheet_auth)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        gc = gspread.service_account_from_dict(info, scopes=scopes)
        logger.info("Opening WorldPop metadata gsheet")
        self._spreadsheet = gc.open_by_url(configuration["spreadsheet_url"])

    def get_metadata(self) -> Dict[str, Dict]:
        all_metadata = {}
        ind_sheet = self._spreadsheet.worksheet("Indicators")
        for ind_row in ind_sheet.get_values("A2:C"):
            alias = ind_row[0]
            resolution = ind_row[1]
            indicator = ind_row[2]
            metadata = all_metadata.get(alias, {})
            dict_of_lists_add(metadata, "Resolution", resolution)
            dict_of_lists_add(metadata, "Indicator", indicator)
            sheet = self._spreadsheet.worksheet(alias)
            for row in sheet.get_values("A2:B"):
                metadata[row[0]] = row[1]
            all_metadata[alias] = metadata
        return all_metadata
