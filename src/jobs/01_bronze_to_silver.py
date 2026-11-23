import uuid
import mysql.connector
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.utils.spark_session import get_spark_session, load_config
from src.utils.dq_engine import IndiaPharmaDQEngine

def log_audit(cfg: dict, job_name: str, domain: str, read_cnt: int, valid_cnt: int, bad_cnt: int):
    m = cfg["mysql"]
    conn = mysql.connector.connect(host=m["host"], port=m["port"], user=m["user"], password=m["password"], database=m["database"])
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO etl_pipeline_audit (run_id, job_name, domain, records_read, records_valid, records_quarantined, status) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (str(uuid.uuid4()), job_name, domain, read_cnt, valid_cnt, bad_cnt, "SUCCESS")
    )
    conn.commit()
    cur.close()
    conn.close()

def run():
    cfg = load_config()
    spark = get_spark_session("Cadway_01_Bronze_To_Silver")
    base = cfg["storage"]["base_path"]
    salt = cfg["project"]["hipaa_abha_salt"]

    # 1. CLINICAL TRIALS
    clin_schema = StructType([
        StructField("obs_id", StringType()), StructField("abha_raw_id", StringType()),
        StructField("cdsco_drug_code", StringType()), StructField("ctri_trial_id", StringType()),
        StructField("trial_phase", StringType()), StructField("state_code", StringType()),
        StructField("obs_date", StringType()), StructField("biomarker_level", DoubleType()),
        StructField("adverse_event", StringType()), StructField("gender", StringType()),
        StructField("age", IntegerType()), StructField("comorbidity_risk", StringType()),
        StructField("_corrupt_record", StringType())
    ])
    df_clin = spark.read.option("header", "true").option("mode", "PERMISSIVE").schema(clin_schema).csv(f"{base}/bronze/clinical/")
    read_c = df_clin.count()

    df_clin = IndiaPharmaDQEngine.clean_strings_and_blanks(df_clin)
    df_clin = IndiaPharmaDQEngine.standardize_indian_dates(df_clin, "obs_date")
    df_clin = IndiaPharmaDQEngine.map_values(df_clin, "gender", {"M": "MALE", "MALE": "MALE", "F": "FEMALE", "FEMALE": "FEMALE"}, "OTHER")
    df_clin = IndiaPharmaDQEngine.map_values(df_clin, "adverse_event", {"Y": "1", "YES": "1", "1": "1", "N": "0", "NO": "0", "0": "0"}, "0")
    df_clin = IndiaPharmaDQEngine.deduplicate(df_clin, ["obs_id"], "obs_date")

    clin_rule = (
        F.col("_corrupt_record").isNull() & F.col("obs_id").isNotNull() & F.col("abha_raw_id").isNotNull() &
        F.col("obs_date").isNotNull() & F.col("biomarker_level").between(0.0, 5000.0) & F.col("age").between(18, 105)
    )
    valid_c, bad_c = IndiaPharmaDQEngine.split_quarantine(df_clin, clin_rule)
    valid_c = (
        valid_c.withColumn("abha_token_id", F.sha2(F.concat(F.col("abha_raw_id"), F.lit(salt)), 256))
        .withColumn("adverse_event_flag", F.col("adverse_event").cast("int"))
        .withColumn("age_group", F.when(F.col("age") < 35, "18-34").when(F.col("age") < 60, "35-59").otherwise("60+"))
        .fillna({"comorbidity_risk": "Low"})
        .drop("abha_raw_id", "adverse_event", "age")
    )
    valid_c.write.mode("overwrite").partitionBy("obs_date").parquet(f"{base}/silver/clinical/")
    bad_c.write.mode("overwrite").parquet(f"{base}/quarantine/clinical/")
    log_audit(cfg, "01_bronze_to_silver", "CLINICAL", read_c, valid_c.count(), bad_c.count())

    # 2. MANUFACTURING BATCHES
    mfg_schema = StructType([
        StructField("batch_lot_id", StringType()), StructField("cdsco_drug_code", StringType()),
        StructField("facility_code", StringType()), StructField("plant_name", StringType()),
        StructField("state_code", StringType()), StructField("who_gmp_status", StringType()),
        StructField("mfg_date", StringType()), StructField("input_kg", DoubleType()),
        StructField("output_kg", DoubleType()), StructField("qc_status", StringType()),
        StructField("mfg_cost_inr", DoubleType()), StructField("_corrupt_record", StringType())
    ])
    df_mfg = spark.read.option("header", "true").option("mode", "PERMISSIVE").schema(mfg_schema).csv(f"{base}/bronze/manufacturing/")
    read_m = df_mfg.count()

    df_mfg = IndiaPharmaDQEngine.clean_strings_and_blanks(df_mfg)
    df_mfg = IndiaPharmaDQEngine.standardize_indian_dates(df_mfg, "mfg_date")
    df_mfg = IndiaPharmaDQEngine.map_values(df_mfg, "qc_status", {"PASS": "1", "PASSED": "1", "1": "1", "FAIL": "0", "FAILED": "0", "0": "0"}, "0")
    df_mfg = IndiaPharmaDQEngine.deduplicate(df_mfg, ["batch_lot_id"], "mfg_date")

    mfg_rule = (
        F.col("_corrupt_record").isNull() & F.col("batch_lot_id").isNotNull() & F.col("mfg_date").isNotNull() &
        (F.col("input_kg") > 0) & (F.col("output_kg") >= 0) & (F.col("output_kg") <= F.col("input_kg"))
    )
    valid_m, bad_m = IndiaPharmaDQEngine.split_quarantine(df_mfg, mfg_rule)
    valid_m = (
        valid_m.withColumn("yield_pct", F.round((F.col("output_kg") / F.col("input_kg")) * 100, 2))
        .withColumn("qc_passed", F.col("qc_status").cast("int"))
        .drop("qc_status")
    )
    valid_m.write.mode("overwrite").partitionBy("mfg_date").parquet(f"{base}/silver/manufacturing/")
    bad_m.write.mode("overwrite").parquet(f"{base}/quarantine/manufacturing/")
    log_audit(cfg, "01_bronze_to_silver", "MANUFACTURING", read_m, valid_m.count(), bad_m.count())

    # 3. COMMERCIAL SALES INDIA
    sales_schema = StructType([
        StructField("invoice_id", StringType()), StructField("cdsco_drug_code", StringType()),
        StructField("nmc_reg_number", StringType()), StructField("doctor_specialty", StringType()),
        StructField("prescriber_tier", StringType()), StructField("state_code", StringType()),
        StructField("sale_date", StringType()), StructField("promo_channel", StringType()),
        StructField("strips_sold", IntegerType()), StructField("gross_sales_inr", DoubleType()),
        StructField("marketing_spend_inr", DoubleType()), StructField("_corrupt_record", StringType())
    ])
    df_sales = spark.read.option("header", "true").option("mode", "PERMISSIVE").schema(sales_schema).csv(f"{base}/bronze/sales/")
    read_s = df_sales.count()

    df_sales = IndiaPharmaDQEngine.clean_strings_and_blanks(df_sales)
    df_sales = IndiaPharmaDQEngine.standardize_indian_dates(df_sales, "sale_date")
    df_sales = IndiaPharmaDQEngine.map_values(df_sales, "promo_channel", {"MR": "MR_Visit", "WHATSAPP": "WhatsApp_Detail", "CME": "CME_Event"}, "Digital")
    df_sales = IndiaPharmaDQEngine.deduplicate(df_sales, ["invoice_id"], "sale_date")

    sales_rule = (
        F.col("_corrupt_record").isNull() & F.col("invoice_id").isNotNull() & F.col("nmc_reg_number").isNotNull() &
        F.col("sale_date").isNotNull() & (F.col("strips_sold") > 0) & (F.col("gross_sales_inr") > 0)
    )
    valid_s, bad_s = IndiaPharmaDQEngine.split_quarantine(df_sales, sales_rule)
    valid_s = (
        valid_s.fillna({"marketing_spend_inr": 0.0, "prescriber_tier": "Silver"})
        .withColumn("gst_amount_inr", F.round(F.col("gross_sales_inr") * 0.12, 2))
        .withColumn("net_revenue_inr", F.round(F.col("gross_sales_inr") - F.col("gst_amount_inr") - F.col("marketing_spend_inr"), 2))
    )
    valid_s.write.mode("overwrite").partitionBy("sale_date").parquet(f"{base}/silver/sales/")
    bad_s.write.mode("overwrite").parquet(f"{base}/quarantine/sales/")
    log_audit(cfg, "01_bronze_to_silver", "COMMERCIAL_SALES", read_s, valid_s.count(), bad_s.count())

    spark.stop()

if __name__ == "__main__":
    run()
