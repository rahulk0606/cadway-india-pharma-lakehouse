from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class SCDType2Processor:
    @staticmethod
    def compute_row_hash(df: DataFrame, tracked_cols: list) -> DataFrame:
        concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in tracked_cols])
        return df.withColumn("row_hash", F.sha2(concat_expr, 256))

    @staticmethod
    def apply_scd2(
        incoming_df: DataFrame,
        existing_dim_df: DataFrame,
        business_key: str,
        tracked_cols: list,
        effective_date_col: str
    ) -> tuple[DataFrame, DataFrame]:
        staged_df = SCDType2Processor.compute_row_hash(incoming_df, tracked_cols)
        active_dim = existing_dim_df.filter(F.col("is_current") == 1)

        joined = staged_df.alias("src").join(active_dim.alias("tgt"), on=business_key, how="left")

        # Select only incoming columns + row_hash so unionByName schemas match 100%
        src_cols = [F.col(f"src.{c}") for c in staged_df.columns]

        new_entities = (
            joined.filter(F.col(f"tgt.{business_key}").isNull())
            .select(*src_cols)
            .withColumn("effective_start_date", F.col(effective_date_col))
            .withColumn("effective_end_date", F.to_date(F.lit("9999-12-31")))
            .withColumn("is_current", F.lit(1))
        )

        changed_joined = joined.filter(
            F.col(f"tgt.{business_key}").isNotNull() &
            (F.col("src.row_hash") != F.col("tgt.row_hash"))
        )

        changed_new_versions = (
            changed_joined.select(*src_cols)
            .withColumn("effective_start_date", F.col(effective_date_col))
            .withColumn("effective_end_date", F.to_date(F.lit("9999-12-31")))
            .withColumn("is_current", F.lit(1))
        )

        # Grab the first surrogate key column name as a string (not a list!)
        sk_col = [c for c in existing_dim_df.columns if c.endswith("_sk")][0]
        records_to_expire = changed_joined.select(
            F.col(f"tgt.{sk_col}"),
            F.date_sub(F.col(f"src.{effective_date_col}"), 1).alias("new_end_date")
        )

        records_to_insert = new_entities.unionByName(changed_new_versions).drop(effective_date_col)
        return records_to_expire, records_to_insert
