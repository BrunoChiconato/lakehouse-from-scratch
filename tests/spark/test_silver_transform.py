from pyspark.sql import Row
from pyspark.sql.functions import col

from transformation.process_to_silver import transform_raw_data


def test_transform_raw_data_category_cleaning_and_grouping(spark):
    rows = [
        Row(
            id="A",
            title="Paper A",
            summary="...",
            authors=["John"],
            categories=["cs.SE (Software Engineering)"],
            published_date="2024-01-01T10:00:00Z",
            updated_date="2024-01-02T10:00:00Z",
            pdf_url="http://x/a.pdf",
        ),
        Row(
            id="B",
            title="Paper B",
            summary="...",
            authors=["Jane"],
            categories=["cs.AI - something", "cs.CL"],
            published_date="2024-02-01T10:00:00Z",
            updated_date="2024-02-02T10:00:00Z",
            pdf_url="http://x/b.pdf",
        ),
        Row(
            id="C",
            title="Paper C",
            summary="...",
            authors=["Ada"],
            categories=["ml"],
            published_date="2023-05-05T10:00:00Z",
            updated_date="2023-05-06T10:00:00Z",
            pdf_url="http://x/c.pdf",
        ),
    ]
    df = spark.createDataFrame(rows)

    df.createOrReplaceGlobalTempView("arxiv_raw_test")

    out = transform_raw_data(spark, "global_temp.arxiv_raw_test")

    assert set(
        [
            "id",
            "title",
            "summary",
            "authors",
            "categories",
            "published_date",
            "updated_date",
            "pdf_url",
            "publication_year",
        ]
    ).issubset(out.columns)

    c = (
        out.select("id", "categories")
        .where(col("id") == "B")
        .collect()[0]["categories"]
    )
    assert "cs.ai" in c and "cs.cl" in c

    a = (
        out.select("id", "categories")
        .where(col("id") == "A")
        .collect()[0]["categories"]
    )
    assert "cs.se" in a

    m = (
        out.select("id", "categories")
        .where(col("id") == "C")
        .collect()[0]["categories"]
    )
    assert "ml.gen" in m

    yrs = {
        r["id"]: r["publication_year"]
        for r in out.select("id", "publication_year").collect()
    }
    assert yrs == {"A": 2024, "B": 2024, "C": 2023}
