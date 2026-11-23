import pytest
from datetime import date
from pyspark.sql import SparkSession
from src.utils.scd2_engine import SCDType2Processor


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test_SCD2").getOrCreate()


def test_scd2_history_chain(spark):
    existing = spark.createDataFrame([
        (1, "NMC-MH-99999", "Oncology", "Silver", "old_hash", date(2026, 4, 10), date(9999, 12, 31), 1)
    ], ["doctor_sk", "nmc_reg_number", "specialty", "prescriber_tier", "row_hash", "effective_start_date", "effective_end_date", "is_current"])
    existing = SCDType2Processor.compute_row_hash(existing, ["specialty", "prescriber_tier"])

    incoming = spark.createDataFrame([
        ("NMC-MH-99999", "Oncology", "Platinum", date(2026, 7, 15))
    ], ["nmc_reg_number", "specialty", "prescriber_tier", "eff_dt"])

    to_expire, to_insert = SCDType2Processor.apply_scd2(
        incoming, existing, "nmc_reg_number", ["specialty", "prescriber_tier"], "eff_dt"
    )
    exp = to_expire.collect()
    ins = to_insert.collect()

    assert len(exp) == 1 and str(exp[0]["new_end_date"]) == "2026-07-14"
    assert len(ins) == 1 and ins[0]["prescriber_tier"] == "Platinum" and ins[0]["is_current"] == 1
