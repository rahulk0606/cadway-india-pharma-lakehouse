import os
import glob
import yaml
from pyspark.sql import SparkSession



def load_config():
    with open("config/pipeline_config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    if os.getenv("MYSQL_HOST"):
        cfg["mysql"]["host"] = os.getenv("MYSQL_HOST")
    if os.getenv("STORAGE_BASE_PATH"):
        cfg["storage"]["base_path"] = os.getenv("STORAGE_BASE_PATH")
    return cfg


def get_mysql_jdbc_details(cfg: dict) -> tuple[str, dict]:
    m = cfg["mysql"]
    url = f"jdbc:mysql://{m['host']}:{m['port']}/{m['database']}?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
    props = {
        "user": m["user"],
        "password": m["password"],
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": str(m["batch_size"])
    }
    return url, props


def get_spark_session(app_name: str) -> SparkSession:
    jars = ",".join(glob.glob("/app/jars/*.jar"))
    spark_builder: SparkSession.Builder = SparkSession.builder  # type: ignore
    builder = (
        spark_builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if jars:
        builder = (
            builder
            .config("spark.jars", jars)
            .config("spark.driver.extraClassPath", jars.replace(",", ":"))
            .config("spark.executor.extraClassPath", jars.replace(",", ":"))
        )
    return builder.getOrCreate()
