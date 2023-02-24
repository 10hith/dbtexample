# https://geoportal.statistics.gov.uk/search?collection=Dataset&sort=-created&tags=all(LUP_LSOA_2021_LAD)

import polars as pl
from utils.params import CENS_STAGING_PATH


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    # Lazy read the csv file
    pl_df_raw = pl.scan_csv(
        f"{CENS_STAGING_PATH}/lsoa11_to_21/*.csv",
        has_header=True,
        ignore_errors=True
    )
    pl_df = pl_df_raw.select([
        pl.col("F_LSOA11CD").alias("lsoa11cd"),
        pl.col("LSOA11NM").alias("lsoa11nm"),
        pl.col("LSOA21CD").alias("lsoa21cd"),
        pl.col("LSOA21NM").alias("lsoa21nm"),
        pl.col("LAD22CD").alias("lad22cd"),
        pl.col("LAD22NM").alias("lad22nm")
    ])
    pdf = pl_df.collect().to_pandas()
    return pdf