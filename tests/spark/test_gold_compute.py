from pyspark.sql import Row
from transformation.build_gold_layer import compute_publication_trends_df


def test_compute_publication_trends_df_join_and_aggregation(spark):
    papers = spark.createDataFrame(
        [
            Row(id="1", publication_year=2024, categories=["cs.se", "cs.ai"]),
            Row(id="2", publication_year=2024, categories=["cs.ai"]),
            Row(id="3", publication_year=2023, categories=["cs.cl"]),
        ]
    )

    dim = spark.createDataFrame(
        [
            Row(category_code="cs.SE", category_name="Software Engineering"),
            Row(category_code="cs.AI", category_name="Artificial Intelligence"),
            Row(category_code="cs.CL", category_name="Computation and Language"),
        ]
    )

    fact_df = compute_publication_trends_df(papers, dim)

    r = {
        (row["publication_year"], row["category_name"]): row["paper_count"]
        for row in fact_df.collect()
    }

    assert r[(2024, "Artificial Intelligence")] == 2
    assert r[(2024, "Software Engineering")] == 1
    assert r[(2023, "Computation and Language")] == 1

    assert fact_df.filter(fact_df.category_name.isNull()).count() == 0
