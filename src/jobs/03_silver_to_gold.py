from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session, load_config, get_mysql_jdbc_details


def run():
    cfg = load_config()
    spark = get_spark_session("Cadway_03_Silver_To_Gold")
    base = cfg["storage"]["base_path"]
    jdbc_url, props = get_mysql_jdbc_details(cfg)

    clin = spark.read.parquet(f"{base}/silver/clinical/")
    mfg = spark.read.parquet(f"{base}/silver/manufacturing/")
    sales = spark.read.parquet(f"{base}/silver/sales/")

    dim_prod = spark.read.jdbc(jdbc_url, "dim_product_scd2", properties=props)
    dim_pat = spark.read.jdbc(jdbc_url, "dim_patient_scd2", properties=props)
    dim_fac = spark.read.jdbc(jdbc_url, "dim_facility_scd2", properties=props)
    dim_doc = spark.read.jdbc(jdbc_url, "dim_doctor_scd2", properties=props)
    dim_geo = spark.read.jdbc(jdbc_url, "dim_geography_india", properties=props)
    dim_date = spark.read.jdbc(jdbc_url, "dim_date", properties=props)

    # 1. Fact Clinical Observations (Idempotent + Point-in-time SCD2 join)
    existing_clin_ids = spark.read.jdbc(jdbc_url, "(SELECT obs_id FROM fact_clinical_observations) t", properties=props)
    fact_clin = (
        clin.alias("c")
        .join(existing_clin_ids, "obs_id", "left_anti")
        .join(F.broadcast(dim_prod).alias("p"), (F.col("c.cdsco_drug_code") == F.col("p.cdsco_drug_code")) & F.col("c.obs_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date")))
        .join(dim_pat.alias("pt"), (F.col("c.abha_token_id") == F.col("pt.abha_token_id")) & F.col("c.obs_date").between(F.col("pt.effective_start_date"), F.col("pt.effective_end_date")))
        .join(F.broadcast(dim_geo).alias("g"), "state_code")
        .select(
            "c.obs_id", F.date_format("c.obs_date", "yyyyMMdd").cast("int").alias("date_sk"),
            "p.product_sk", "pt.patient_sk", "g.geo_sk", "c.ctri_trial_id", "c.trial_phase", "c.biomarker_level", "c.adverse_event_flag"
        )
    )
    if fact_clin.count() > 0:
        fact_clin.write.jdbc(jdbc_url, "fact_clinical_observations", mode="append", properties=props)

    # 2. Fact Manufacturing Batches (Idempotent + Point-in-time SCD2 join)
    existing_mfg_ids = spark.read.jdbc(jdbc_url, "(SELECT batch_lot_id FROM fact_mfg_batches) t", properties=props)
    fact_mfg = (
        mfg.alias("m")
        .join(existing_mfg_ids, "batch_lot_id", "left_anti")
        .join(F.broadcast(dim_prod).alias("p"), (F.col("m.cdsco_drug_code") == F.col("p.cdsco_drug_code")) & F.col("m.mfg_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date")))
        .join(F.broadcast(dim_fac).alias("f"), (F.col("m.facility_code") == F.col("f.facility_code")) & F.col("m.mfg_date").between(F.col("f.effective_start_date"), F.col("f.effective_end_date")))
        .join(F.broadcast(dim_geo).alias("g"), "state_code")
        .select(
            "m.batch_lot_id", F.date_format("m.mfg_date", "yyyyMMdd").cast("int").alias("date_sk"),
            "p.product_sk", "f.facility_sk", "g.geo_sk", "m.input_kg", "m.output_kg", "m.yield_pct", "m.qc_passed", "m.mfg_cost_inr"
        )
    )
    if fact_mfg.count() > 0:
        fact_mfg.write.jdbc(jdbc_url, "fact_mfg_batches", mode="append", properties=props)

    # 3. Fact Commercial Sales India (Idempotent + Point-in-time SCD2 join)
    existing_sales_ids = spark.read.jdbc(
        jdbc_url,
        "(SELECT invoice_id FROM fact_commercial_sales_india) t",
        properties=props)
    fact_sales = (
        sales.alias("s")
        .join(existing_sales_ids, "invoice_id", "left_anti")
        .join(F.broadcast(dim_prod).alias("p"), (F.col("s.cdsco_drug_code") == F.col("p.cdsco_drug_code")) & F.col("s.sale_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date")))
        .join(F.broadcast(dim_doc).alias("d"), (F.col("s.nmc_reg_number") == F.col("d.nmc_reg_number")) & F.col("s.sale_date").between(F.col("d.effective_start_date"), F.col("d.effective_end_date")))
        .join(F.broadcast(dim_geo).alias("g"), "state_code")
        .select(
            "s.invoice_id", F.date_format("s.sale_date", "yyyyMMdd").cast("int").alias("date_sk"),
            "p.product_sk", "d.doctor_sk", "g.geo_sk", "s.promo_channel", "s.strips_sold",
            "s.gross_sales_inr", "s.gst_amount_inr", "s.marketing_spend_inr", "s.net_revenue_inr"
        )
    )
    if fact_sales.count() > 0:
        fact_sales.write.jdbc(jdbc_url, "fact_commercial_sales_india", mode="append", properties=props)

    # 4. Executive Cross-Domain KPI Mart (Indian FY & Zone Level)
    clin_kpi = (
        clin.join(F.broadcast(dim_geo), "state_code")
        .withColumn("kpi_month", F.date_format("obs_date", "yyyyMM").cast("int"))
        .groupBy("kpi_month", "zone", "cdsco_drug_code")
        .agg(
            F.countDistinct("abha_token_id").alias("active_trial_patients"),
            F.round(F.avg("adverse_event_flag") * 100, 2).alias("adverse_event_rate_pct")
        )
    )

    mfg_kpi = (
        mfg.join(F.broadcast(dim_geo), "state_code")
        .withColumn("kpi_month", F.date_format("mfg_date", "yyyyMM").cast("int"))
        .groupBy("kpi_month", "zone", "cdsco_drug_code")
        .agg(
            F.round(F.avg("yield_pct"), 2).alias("avg_batch_yield_pct"),
            F.round(F.avg("qc_passed") * 100, 2).alias("qc_pass_rate_pct")
        )
    )

    sales_kpi = (
        sales.join(F.broadcast(dim_geo), "state_code")
        .join(F.broadcast(dim_date), sales.sale_date == dim_date.full_date)
        .withColumn("kpi_month", F.date_format("sale_date", "yyyyMM").cast("int"))
        .groupBy("indian_fy", "kpi_month", "zone", "cdsco_drug_code")
        .agg(
            F.sum("strips_sold").alias("total_strips_sold"),
            F.round(F.sum("net_revenue_inr") / 100000.0, 2).alias("net_revenue_lakhs_inr"),
            F.round(
                F.when(F.sum("marketing_spend_inr") > 0, F.sum("net_revenue_inr") / F.sum("marketing_spend_inr"))
                .otherwise(F.lit(0.0)),
                2
            ).alias("marketing_roi")

        )
    )

    exec_mart = (
        sales_kpi
        .join(mfg_kpi, ["kpi_month", "zone", "cdsco_drug_code"], "left")
        .join(clin_kpi, ["kpi_month", "zone", "cdsco_drug_code"], "left")
        .join(F.broadcast(dim_prod.filter("is_current = 1")), "cdsco_drug_code", "inner")
        .select(
            "indian_fy", "kpi_month", "zone", "brand_name", "therapeutic_area",
            "active_trial_patients", "adverse_event_rate_pct",
            "avg_batch_yield_pct", "qc_pass_rate_pct",
            "total_strips_sold", "net_revenue_lakhs_inr", "marketing_roi"
        )
    )
    exec_mart.write.jdbc(jdbc_url, "mart_cadway_india_kpis", mode="overwrite", properties=props)
    spark.stop()


if __name__ == "__main__":
    run()
