import logging

from hdx.api.configuration import Configuration
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Sheet:
    def __init__(self, retriever: Retrieve, configuration: Configuration):
        self._retriever = retriever
        self._url = configuration["spreadsheet_url"]
        self._aliases = configuration["aliases"]

    def get_metadata(self) -> dict[str, dict]:
        logger.info("Reading WorldPop metadata gsheet")
        all_metadata = {}
        for alias, alias_config in self._aliases.items():
            _, rows = self._retriever.get_tabular_rows(
                self._url.format(gid=alias_config["gid"]),
                filename=f"sheet_{alias}.csv",
                logstr=f"metadata sheet {alias}",
                format="csv",
            )
            all_metadata[alias] = {row[0]: row[1] for row in rows}
        return all_metadata
