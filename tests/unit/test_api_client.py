import pytest
import requests
from unittest.mock import MagicMock

from ingestion.api_client import ArxivAPIClient
from config import settings


def test_arxiv_api_client_initialization():
    """
    Verifica se o client usa a base URL de settings.
    """
    client = ArxivAPIClient()
    assert client.base_url == settings.ARXIV_BASE_URL


def test_fetch_batch_success(mocker):
    """
    Verifica o fetch_batch em caso de sucesso.
    Faz patch em requests.Session.get (não requests.get).
    """
    mock_get = mocker.patch("ingestion.api_client.requests.Session.get")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"<xml>Fake article data</xml>"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    client = ArxivAPIClient()

    result = client.fetch_batch(category="cs.AI", start=0, batch_size=100)

    expected_params = {
        "search_query": "cat:cs.AI",
        "sortBy": settings.ARXIV_SORT_BY,
        "sortOrder": settings.ARXIV_SORT_ORDER,
        "start": 0,
        "max_results": 100,
    }
    mock_get.assert_called_once_with(
        settings.ARXIV_BASE_URL, params=expected_params, timeout=client.timeout
    )
    mock_response.raise_for_status.assert_called_once()

    assert result == b"<xml>Fake article data</xml>"


def test_fetch_batch_failure_raises_exception(mocker):
    """
    Verifica se fetch_batch propaga exceção quando a chamada falha.
    """
    mocker.patch(
        "ingestion.api_client.requests.Session.get",
        side_effect=requests.exceptions.RequestException("Test network error"),
    )

    client = ArxivAPIClient()

    with pytest.raises(requests.exceptions.RequestException) as excinfo:
        client.fetch_batch(category="cs.AI", start=0, batch_size=100)

    assert "Test network error" in str(excinfo.value)
