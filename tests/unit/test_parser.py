import xml.etree.ElementTree as ET
import pytest

from ingestion.main import parse_xml_entry, ATOM_NAMESPACE
from contracts.arxiv_contract import ArxivArticle


@pytest.fixture
def sample_xml_entry_string():
    """Provides a sample XML <entry> block as a string."""
    return f"""
    <entry xmlns="{ATOM_NAMESPACE.strip("{}")}">
        <id>http://arxiv.org/abs/2401.12345v1</id>
        <updated>2024-01-02T15:00:00Z</updated>
        <published>2024-01-01T12:00:00Z</published>
        <title>A Great Paper on Software Engineering</title>
        <summary>This is the summary of the paper. It is very insightful.</summary>
        <author><name>Guido van Rossum</name></author>
        <category term="cs.SE"/>
        <link title="pdf" href="http://arxiv.org/pdf/2401.12345v1" rel="related" type="application/pdf"/>
    </entry>
    """


def test_parse_xml_entry_happy_path(sample_xml_entry_string):
    """
    Unit Test: Verifies that a valid XML entry is correctly parsed into an ArxivArticle object.
    """
    xml_element = ET.fromstring(sample_xml_entry_string)
    parsed_article = parse_xml_entry(xml_element)

    assert isinstance(parsed_article, ArxivArticle)
    assert parsed_article.id == "2401.12345v1"
    assert parsed_article.title == "A Great Paper on Software Engineering"
    assert parsed_article.authors == ["Guido van Rossum"]
    assert parsed_article.categories == ["cs.SE"]


def test_parse_xml_entry_handles_missing_optional_element():
    """
    Unit Test: Verifies that parsing does not fail if an optional element (like pdf_url) is missing.
    """
    xml_string_no_pdf = f"""
    <entry xmlns="{ATOM_NAMESPACE.strip("{}")}">
        <id>http://arxiv.org/abs/2401.54321v1</id>
        <title>A Paper Without a PDF Link</title>
        <summary>Summary here.</summary>
        <author><name>Ada Lovelace</name></author>
        <category term="cs.PL"/>
        <published>2024-02-01T10:00:00Z</published>
        <updated>2024-02-01T10:00:00Z</updated>
    </entry>
    """
    xml_element = ET.fromstring(xml_string_no_pdf)
    parsed_article = parse_xml_entry(xml_element)

    assert parsed_article.id == "2401.54321v1"
    assert parsed_article.pdf_url == ""
