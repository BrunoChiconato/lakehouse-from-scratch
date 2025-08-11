import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings

logger = logging.getLogger(__name__)


class ArxivAPIClient:
    """A client to interact with the arXiv API (resilient with retries)."""

    def __init__(self, timeout: int = 30):
        self.base_url = settings.ARXIV_BASE_URL
        self.timeout = timeout

        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def fetch_batch(self, category: str, start: int, batch_size: int) -> bytes:
        """
        Fetches a single batch of articles for a given category.
        Returns the response content as bytes.
        """
        params = {
            "search_query": f"cat:{category}",
            "sortBy": settings.ARXIV_SORT_BY,
            "sortOrder": settings.ARXIV_SORT_ORDER,
            "start": start,
            "max_results": batch_size,
        }

        logger.info(
            f"Fetching batch for '{category}', starting from index {start} (size={batch_size})..."
        )
        response = self.session.get(self.base_url, params=params, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request to arXiv API failed: {e}")
            raise
        return response.content
