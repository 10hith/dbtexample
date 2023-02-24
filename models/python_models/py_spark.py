from pyspark.sql import SparkSession
import json
from pathlib import Path
from pyspark.sql import DataFrame


def get_local_spark_session(app_name: str = "SparkTest"):
    """
    Creates a local spark session.
            # .config("spark.sql.shuffle.partitions", f"{SPARK_NUM_PARTITIONS}") \
            spark.debug.maxToStringFields=100
            spark.conf.set("spark.sql.debug.maxToStringFields", 100)
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory','32G') \
    Need to add more configuration
    :param app_name:
    :return:
    """
    spark = SparkSession \
        .builder \
        .appName(f"dbtapp") \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.debug.maxToStringFields', 100) \
        .getOrCreate()
    return spark


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pyspark"]
    )
    spark = get_local_spark_session("dbtTest")
    df = spark.createDataFrame(
        [
            (1, "foo"),  # create your data here, be consistent in the types.
            (2, "bar"),
        ],
        ["id", "label"]  # add your column names here
    )
    df.printSchema()
    return df.limit(100).toPandas()