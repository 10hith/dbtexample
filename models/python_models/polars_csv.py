import polars as pl


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    df = pl.read_csv("/mnt/c/Users/lohith/Downloads/crime_data_Archive_IMP/2015-12/2013-01/*.csv")
    return df

