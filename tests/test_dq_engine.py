import pytest
from pyspark.sql import SparkSession
from src.utils.dq_engine import IndiaPharmaDQEngine


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("Test_DQ").getOrCreate()


def test_indian_date_formats(spark):
    df = spark.createDataFrame([("2026-08-15",), ("15-08-2026",), ("15/08/2026",), ("BAD",)], ["dt"])
    res = [str(r["dt"]) if r["dt"] else None for r in IndiaPharmaDQEngine.standardize_indian_dates(df, "dt").collect()]
    assert res == ["2026-08-15", "2026-08-15", "2026-08-15", None]


def test_quarantine_split(spark):
    df = spark.createDataFrame([("LOT-1", 94.5), ("LOT-2", -5.0)], ["lot", "yield"])
    valid, bad = IndiaPharmaDQEngine.split_quarantine(df, df["yield"] > 0)
    assert valid.count() == 1
    assert bad.count() == 1
