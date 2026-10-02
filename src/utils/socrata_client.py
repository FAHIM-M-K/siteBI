"""
Reusable Socrata Open Data API Client
Handles pagination, rate limiting/retries, watermark filtering via SoQL ($where),
and authentication tokens for NYC Open Data and NY State Open Data portals.
"""

import time
import logging
from typing import Dict, Any, List, Optional, Iterator
import requests

from src.utils.config import (
    DOMAIN_NYC_OPEN_DATA,
    NYC_OPEN_DATA_APP_TOKEN,
)

logger = logging.getLogger(__name__)


class SocrataClient:
    """
    Client for querying Socrata endpoints (NYC Open Data / NY State Open Data)
    with resilient pagination, backoff, and SoQL query composition.
    """

    def __init__(
        self,
        domain: str = DOMAIN_NYC_OPEN_DATA,
        app_token: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.domain = domain.replace("https://", "").replace("http://", "").strip("/")
        self.app_token = app_token or NYC_OPEN_DATA_APP_TOKEN
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_url = f"https://{self.domain}/resource"

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": "SiteBI-DataPipeline/1.0",
        }
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    def fetch_page(
        self,
        dataset_id: str,
        limit: int = 50000,
        offset: int = 0,
        where: Optional[str] = None,
        order: Optional[str] = None,
        select: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetches a single page of records from the given dataset ID.
        Includes automatic retry with exponential backoff.
        """
        clean_dataset_id = dataset_id.replace(".json", "")
        endpoint = f"{self.base_url}/{clean_dataset_id}.json"

        params: Dict[str, Any] = {
            "$limit": limit,
            "$offset": offset,
        }
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order
        if select:
            params["$select"] = select

        headers = self._get_headers()

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )

                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        return data
                    logger.warning("Unexpected response format: %s", type(data))
                    return []

                # Handle rate limiting (429) or transient server errors (500, 502, 503, 504)
                if response.status_code in (429, 500, 502, 503, 504):
                    sleep_time = self.backoff_factor ** attempt
                    logger.warning(
                        "Received HTTP %d from %s. Retrying in %.1fs (attempt %d/%d)...",
                        response.status_code, endpoint, sleep_time, attempt, self.max_retries
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()

            except (requests.RequestException, ValueError) as err:
                if attempt == self.max_retries:
                    logger.error("Failed to fetch from %s after %d attempts: %s", endpoint, attempt, err)
                    raise
                sleep_time = self.backoff_factor ** attempt
                time.sleep(sleep_time)

        return []

    def fetch_all(
        self,
        dataset_id: str,
        page_size: int = 50000,
        max_records: Optional[int] = None,
        where: Optional[str] = None,
        order: Optional[str] = None,
        select: Optional[str] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Generator yielding pages of records until all available matching records are retrieved,
        or max_records is reached.
        """
        offset = 0
        total_fetched = 0

        while True:
            current_limit = page_size
            if max_records is not None:
                remaining = max_records - total_fetched
                if remaining <= 0:
                    break
                current_limit = min(page_size, remaining)

            logger.info("Fetching %s: offset=%d, limit=%d", dataset_id, offset, current_limit)
            page = self.fetch_page(
                dataset_id=dataset_id,
                limit=current_limit,
                offset=offset,
                where=where,
                order=order,
                select=select,
            )

            if not page:
                break

            yield page
            total_fetched += len(page)
            offset += len(page)

            # If fewer records returned than requested, we've reached the end
            if len(page) < current_limit:
                break
