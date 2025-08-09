import pytest
from datetime import datetime
from pydantic import ValidationError

from contracts.arxiv_contract import ArxivArticle


@pytest.fixture
def valid_article_data():
    """Provides a dictionary of valid article data for tests."""
    return {
        "id": "2401.0001v1",
        "title": "A Test Paper on AI",
        "summary": "This is the summary of the test paper.",
        "authors": ["John Doe", "Jane Smith"],
        "categories": ["cs.AI"],
        "published_date": "2024-01-01T10:00:00Z",
        "updated_date": "2024-01-02T12:00:00Z",
        "pdf_url": "http://example.com/test.pdf",
    }


def test_arxiv_article_successful_creation(valid_article_data):
    """
    Unit Test: Verifies that an ArxivArticle object is created successfully
    with valid data.
    """
    article = ArxivArticle(**valid_article_data)

    assert article.id == "2401.0001v1"
    assert isinstance(article.published_date, datetime)
    assert article.authors[0] == "John Doe"


def test_arxiv_article_fails_with_empty_title(valid_article_data):
    """
    Unit Test: Ensures the contract rejects data where a required text field is empty.
    """
    invalid_data = valid_article_data.copy()
    invalid_data["title"] = ""

    with pytest.raises(ValidationError) as error_info:
        ArxivArticle(**invalid_data)

    assert "title" in str(error_info.value)


def test_arxiv_article_fails_with_empty_authors_list(valid_article_data):
    """
    Unit Test: Ensures our custom validator rejects an empty list for 'authors'.
    """
    invalid_data = valid_article_data.copy()
    invalid_data["authors"] = []  # Lista de autores vazia

    with pytest.raises(ValidationError) as error_info:
        ArxivArticle(**invalid_data)

    assert "authors" in str(error_info.value)
    assert "List cannot be empty" in str(error_info.value)
