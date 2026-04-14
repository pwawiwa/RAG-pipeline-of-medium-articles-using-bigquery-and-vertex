import pytest
from medium_rag_utils.data_contracts import validate_medium_article, DataContractError

def test_validate_valid_article():
    record = {
        "article_url": "https://medium.com/@test/my-article",
        "title": "Test Title",
        "page_content": "A" * 250,  # > 200 chars
        "published_date": "2026-01-01"
    }
    assert validate_medium_article(record) is True

def test_validate_missing_field():
    record = {
        "article_url": "https://medium.com/@test/my-article",
        "title": "Test Title",
        # Missing page_content
        "published_date": "2026-01-01"
    }
    with pytest.raises(DataContractError, match="Missing required field: page_content"):
        validate_medium_article(record)

def test_validate_content_too_short():
    record = {
        "article_url": "https://medium.com/@test/my-article",
        "title": "Test Title",
        "page_content": "Too short",
        "published_date": "2026-01-01"
    }
    with pytest.raises(DataContractError, match="Article content too short"):
        validate_medium_article(record)

def test_validate_invalid_url():
    record = {
        "article_url": "invalid-url",
        "title": "Test Title",
        "page_content": "A" * 250,
        "published_date": "2026-01-01"
    }
    with pytest.raises(DataContractError, match="Invalid article URL"):
        validate_medium_article(record)

def test_validate_invalid_date_format():
    record = {
        "article_url": "https://medium.com/@test/my-article",
        "title": "Test Title",
        "page_content": "A" * 250,
        "published_date": "01/01/2026" # Invalid format
    }
    with pytest.raises(DataContractError, match="Invalid date format"):
        validate_medium_article(record)
