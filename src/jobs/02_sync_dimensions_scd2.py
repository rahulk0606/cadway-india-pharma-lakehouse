import mysql.connector
from pyspark.sql import Window, functions as F
from src.utils.spark_session import get_spark_session, load_config, get_mysql_jdbc_details
from src.utils.scd2_engine import SCDType2Processor


def sync_scd2_dimension(spark, cfg, stage_df, table_name, sk_col, biz_key, tracked_cols, eff_date_col):
    jdbc_url, props = get_mysql_jdbc_details(cfg)
    m = cfg["mysql"]

    # 1. Deduplicate same-day events per business key
    day_win = Window.partitionBy(biz_key, eff_date_col).orderBy(F.col(eff_date_col).desc())
    dedup_df = stage_df.withColumn("_rn", F.row_number().over(day_win)).filter("_rn = 1").drop("_rn")

    # 2. Compute SHA-256 hash and filter out consecutive unchanged states within the incoming batch
    hashed_df = SCDType2Processor.compute_row_hash(dedup_df, tracked_cols)
    lag_win = Window.partitionBy(biz_key).orderBy(F.col(eff_date_col).asc())
    state_changes_df = (
        hashed_df
        .withColumn("_prev_hash", F.lag("row_hash").over(lag_win))
        .filter(F.col("_prev_hash").isNull() | (F.col("row_hash") != F.col("_prev_hash")))
        .drop("_prev_hash")
    )

    # 3. Build complete SCD2 effective_start_date, effective_end_date, and is_current using LEAD()
    lead_win = Window.partitionBy(biz_key).orderBy(F.col(eff_date_col).asc())
    scd2_ready_df = (
        state_changes_df
        .withColumn("effective_start_date", F.col(eff_date_col))
        .withColumn("_next_start", F.lead(eff_date_col).over(lead_win))
        .withColumn(
            "effective_end_date",
            F.when(F.col("_next_start").isNull(), F.to_date(F.lit("9999-12-31")))
             .otherwise(F.date_sub(F.col("_next_start"), 1))
        )
        .withColumn("is_current", F.when(F.col("_next_start").isNull(), F.lit(1)).otherwise(F.lit(0)))
        .drop("_next_start", eff_date_col)
    )

    # 4. Expire any previously active rows in MySQL if re-running incrementally
    existing_active = spark.read.jdbc(
        jdbc_url,
        f"(SELECT {sk_col}, {biz_key}, row_hash FROM {table_name} WHERE is_current = 1) t",
        properties=props)
    earliest_new = (
        scd2_ready_df.groupBy(biz_key)
        .agg(F.min("effective_start_date").alias("min_start"), F.first("row_hash").alias("first_hash"))
    )
    to_expire = (
        existing_active.alias("e")
        .join(earliest_new.alias("n"), biz_key, "inner")
        .filter(F.col("e.row_hash") != F.col("n.first_hash"))
        .select(F.col(f"e.{sk_col}"), F.date_sub(F.col("n.min_start"), 1).alias("new_end"))
        .collect()
    )

    if to_expire:
        conn = mysql.connector.connect(
            host=m["host"],
            port=m["port"],
            user=m["user"],
            password=m["password"],
            database=m["database"])
        cur = conn.cursor()
        cur.executemany(
            f"UPDATE {table_name} SET is_current = 0, effective_end_date = %s WHERE {sk_col} = %s",
            [(r["new_end"], r[sk_col]) for r in to_expire]
        )
        conn.commit()
        cur.close()
        conn.close()

    # Exclude keys that already exist with identical hash
    unchanged_keys = (
        existing_active.alias("e")
        .join(earliest_new.alias("n"), biz_key, "inner")
        .filter(F.col("e.row_hash") == F.col("n.first_hash"))
        .select(biz_key)
    )
    final_insert_df = scd2_ready_df.join(unchanged_keys, biz_key, "left_anti")

    if final_insert_df.count() > 0:
        final_insert_df.write.jdbc(jdbc_url, table_name, mode="append", properties=props)


def run():
    cfg = load_config()
    spark = get_spark_session("Cadway_02_Sync_Dimensions_SCD2")
    base = cfg["storage"]["base_path"]
    jdbc_url, props = get_mysql_jdbc_details(cfg)

    geo_df = spark.read.jdbc(jdbc_url, "dim_geography_india", properties=props).select("geo_sk", "state_code")

    # 1. Sync Dim_Patient_SCD2
    clin = spark.read.parquet(f"{base}/silver/clinical/")
    pat_stage = (
        clin.join(F.broadcast(geo_df), "state_code", "inner")
        .select("abha_token_id", "geo_sk", "age_group", "gender", "comorbidity_risk", F.col("obs_date").alias("eff_dt"))
    )
    sync_scd2_dimension(spark, cfg, pat_stage, "dim_patient_scd2", "patient_sk",
                        "abha_token_id", ["geo_sk", "comorbidity_risk"], "eff_dt")

    # 2. Sync Dim_Facility_SCD2
    mfg = spark.read.parquet(f"{base}/silver/manufacturing/")
    fac_stage = (
        mfg.join(F.broadcast(geo_df), "state_code", "inner")
        .select("facility_code", "plant_name", "geo_sk", "who_gmp_status", F.col("mfg_date").alias("eff_dt"))
    )
    sync_scd2_dimension(spark, cfg, fac_stage, "dim_facility_scd2", "facility_sk",
                        "facility_code", ["who_gmp_status", "geo_sk"], "eff_dt")

    # 3. Sync Dim_Doctor_SCD2
    sales = spark.read.parquet(f"{base}/silver/sales/")
    doc_stage = (
        sales.join(F.broadcast(geo_df), "state_code", "inner")
        .select("nmc_reg_number", "geo_sk", F.col("doctor_specialty").alias("specialty"), "prescriber_tier", F.col("sale_date").alias("eff_dt"))
    )
    sync_scd2_dimension(spark, cfg, doc_stage, "dim_doctor_scd2", "doctor_sk",
                        "nmc_reg_number", ["geo_sk", "specialty", "prescriber_tier"], "eff_dt")

    spark.stop()


if __name__ == "__main__":
    run()
