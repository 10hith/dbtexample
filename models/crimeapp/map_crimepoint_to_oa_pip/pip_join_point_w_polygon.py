from utils.params import CENS_STAGING_PATH
from utils.dbutils import pl_df_from_query, sdf_df_from_query
from utils.spark_utils import get_spatial_spark_session
from pyspark.sql import functions as f
import pandas as pd
# Long running


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    spark = get_spatial_spark_session("pip_join_point_w_polygon")
    # Below dbt.ref returns a pandas df. Dont have to specify connection details at all
    crime_point_index_pdf = dbt.ref("crime_point_index")
    # partition_oa_by_ladcd = dbt.ref("partition_oa_by_ladcd")
    crime_point_index = spark.createDataFrame(crime_point_index_pdf)

    # Read the lad partitioned oa geo; takes 2.5hrs to run, still issues
    oa_geo_lad_partition = spark.read.parquet(f"{CENS_STAGING_PATH}/temp/oa_geo_lad_partition/")

    oa_geo_lad_partition.alias("geo").join(
        crime_point_index.alias("crimeIndex"),
        f.expr("""
            ST_Contains(geo.poly_4326, ST_Point(crimeIndex.lat, crimeIndex.lon)) and
            crimeIndex.lad22cd = geo.lad22cd
        """),
        how='right'
    ) \
        .selectExpr(
        "oa21cd",
        "crimeIndex.lad22cd",
        "lat",
        "lon",
        "crimeIndex.lsoa11cd"
    ) \
        .write.mode("overwrite") \
        .parquet(f"{CENS_STAGING_PATH}/temp/crime_point_index_w_oa/")

    op_dict = {}
    op_dict['loaded'] = "success"
    return pd.DataFrame.from_dict([op_dict])
