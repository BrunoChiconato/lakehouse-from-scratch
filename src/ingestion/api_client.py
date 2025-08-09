import logging
import requests

from config import settings

logger = logging.getLogger(__name__)


class ArxivAPIClient:
    """A client to interact with the arXiv API."""

    def __init__(self):
        self.base_url = settings.ARXIV_BASE_URL

    def fetch_batch(self, category: str, start: int, batch_size: int) -> str:
        """
        Fetches a single batch of articles for a given category.
        Returns the response content as a string.
        """
        params = {
            "search_query": f"cat:{category}",
            "sortBy": settings.ARXIV_SORT_BY,
            "sortOrder": settings.ARXIV_SORT_ORDER,
            "start": start,
            "max_results": batch_size,
        }

        logger.info(f"Fetching batch for '{category}', starting from index {start}...")
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request to arXiv API failed: {e}")
            raise
