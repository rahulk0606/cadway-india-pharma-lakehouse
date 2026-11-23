from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


class IndiaPharmaDQEngine:
    @staticmethod
    def clean_strings_and_blanks(df: DataFrame) -> DataFrame:
        for col_name, dtype in df.dtypes:
            if dtype == "string" and col_name != "_corrupt_record":
                cleaned = F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " ")
                df = df.withColumn(
                    col_name,
                    F.when((cleaned == "") | (F.upper(cleaned).isin("NULL", "N/A", "NONE", "NAN", "NA", "-")), None)
                     .otherwise(cleaned)
                )
        return df

    @staticmethod
    def standardize_indian_dates(df: DataFrame, col_name: str) -> DataFrame:
        parsed = F.coalesce(
            F.to_date(F.col(col_name), "yyyy-MM-dd"),
            F.to_date(F.col(col_name), "dd-MM-yyyy"),
            F.to_date(F.col(col_name), "dd/MM/yyyy"),
            F.to_date(F.col(col_name), "yyyy/MM/dd")
        )
        return df.withColumn(col_name, parsed)

    @staticmethod
    def deduplicate(df: DataFrame, keys: list, order_col: str) -> DataFrame:
        win = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
        return df.withColumn("_rn", F.row_number().over(win)).filter(F.col("_rn") == 1).drop("_rn")

    @staticmethod
    def map_values(df: DataFrame, col_name: str, mapping: dict, default: str = "OTHER") -> DataFrame:
        expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
        return df.withColumn(col_name, F.coalesce(expr[F.upper(F.col(col_name))], F.lit(default)))

    @staticmethod
    def split_quarantine(df: DataFrame, rule_expr) -> tuple[DataFrame, DataFrame]:
        evaluated = df.withColumn("_valid", rule_expr)
        valid_df = evaluated.filter(F.col("_valid") == True).drop("_valid", "_corrupt_record")
        bad_df = evaluated.filter(
            F.col("_valid") == False).withColumn(
            "quarantined_at",
            F.current_timestamp()).drop("_valid")
        return valid_df, bad_df
