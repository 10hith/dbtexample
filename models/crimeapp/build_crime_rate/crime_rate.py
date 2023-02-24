import polars as pl
from utils.dbutils import pl_df_from_query
import pandas as pd


def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    # crime_agg_oa: pl.DataFrame = pl.from_pandas(
    #     dbt.ref("crime_agg_oa")
    # )
    # dbt.ref("crime_agg_oa") of an sql model returns dbConnectionsType

    crime_agg_oa = pl.from_arrow(dbt.ref("crime_agg_oa").arrow())
    pop = pl.from_arrow(dbt.ref("area_pop_cens").arrow())

    oa = crime_agg_oa.groupby(['oacd', 'crime_type_cd', 'year']).agg([
        pl.col('crime_count').sum()
    ]).rename({"oacd": "area_cd"})

    ward = crime_agg_oa.groupby(['wdcd', 'crime_type_cd', 'year']).agg([
        pl.col('crime_count').sum()
    ]).rename({"wdcd": "area_cd"})

    lad = crime_agg_oa.groupby(['ladcd', 'crime_type_cd', 'year']).agg([
        pl.col('crime_count').sum()
    ]).rename({"ladcd": "area_cd"})

    area_cd_stacked_w_pop = oa.vstack(ward).vstack(lad) \
        .join(pop, on="area_cd", how="inner") \
        .with_columns([
            ((pl.col('crime_count') / pl.col('pop')) * 1000).alias('crime_rate')
        ])
    return area_cd_stacked_w_pop.to_pandas()
