from datetime import datetime
from typing import List
from pydantic import BaseModel, field_validator, Field


class ArxivArticle(BaseModel):
    """
    Data contract for a single article fetched from the arXiv API.

    This model defines the expected structure, data types, and basic
    validation rules for the data before it enters the bronze layer.
    """

    id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    authors: List[str]
    categories: List[str]
    published_date: datetime
    updated_date: datetime
    pdf_url: str

    @field_validator("authors", "categories")
    @classmethod
    def check_not_empty(cls, value: List[str]) -> List[str]:
        """Ensures that author and category lists are not empty."""
        if not value:
            raise ValueError("List cannot be empty")
        return value
