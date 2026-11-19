import json
import logging
from typing import Dict

import gspread

from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)


class Sheet:
    def __init__(self, gsheet_auth: str, configuration: Configuration):
        info = json.loads(gsheet_auth)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        gc = gspread.service_account_from_dict(info, scopes=scopes)
        logger.info("Opening WorldPop metadata gsheet")
        self._aliases = configuration["aliases"]
        self._spreadsheet = gc.open_by_url(configuration["spreadsheet_url"])

    def get_metadata(self) -> Dict[str, Dict]:
        all_metadata = {}
        for alias in self._aliases:
            sheet = self._spreadsheet.worksheet(alias)
            all_metadata[alias] = {row[0]: row[1] for row in sheet.get_values("A2:B")}
        return all_metadata
