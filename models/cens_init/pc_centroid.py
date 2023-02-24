import pandas as pd
import glob
import duckdb
import os
import polars as pl
from utils.params import CENS_STAGING_PATH


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    # Lazy read the csv file
    pc_centroid_raw = pl.scan_csv(
        f"{CENS_STAGING_PATH}/pc_centroid/*.csv",
        has_header=True,
        ignore_errors=True
    )
    pc_centroid = pc_centroid_raw.select([
        pl.col("PCD").str.to_lowercase().str.strip().str.replace(' ', '').alias("pc"),
        pl.col("PCD").alias("pc_raw"),
        pl.col("LAT").alias("lat"),
        pl.col("LONG").alias("lon"),
    ])
    pdf = pc_centroid.collect().to_pandas()
    return pdf