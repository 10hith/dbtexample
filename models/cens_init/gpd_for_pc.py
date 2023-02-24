from utils.spark_utils import get_spatial_spark_session
from utils.params import CENS_STAGING_PATH
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter
from pyspark.sql import functions as f
import glob
import pandas as pd
import os
import geopandas as gpd
from utils.dbutils import sdf_df_from_query

# This hasnt been run via DBT


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    # prepare sources - AreaMapping and PcPoly

    spark = get_spatial_spark_session("dbtSpark")
    query = "select pcd, ltla22cd as lad_cd, oa21cd as oa_cd from pc_to_oa_mapping"
    pc_mapping_sdf = sdf_df_from_query('cens.db', query, spark)

    pc_poly = spark.read.\
        parquet(f"/mnt/c/Users/lohith/Downloads/OS_data_sets/processed/postcode_polygons").selectExpr(
            "LOWER(postcode) as area_cd",
            "ST_FlipCoordinates(postcode_poly_4326) as poly_4326_fc"
    )
    # Write polygon data with partition by lad
    pc_poly.join(
        f.broadcast(pc_mapping_sdf),
        on="area_cd",
        how='inner'
    ).write.mode("overwrite") \
        .partitionBy("lad_cd") \
        .parquet(f"{CENS_STAGING_PATH}/geo_parquets/pc")

    # Iterate over partitions, collect spark into geoPandas and write parquet
    file_list = glob.glob(f"{CENS_STAGING_PATH}/geo_parquets/pc/*")

    for file in file_list:
        ladcd = file.split('=')[1]
        print(f"processing {ladcd}")
        pc_poly = spark.read.parquet(file)
        _pdf = pc_poly.selectExpr(
            "area_cd",
            "poly_4326_fc"
        ).toPandas().set_index("area_cd")

        _gpd = gpd.GeoDataFrame(_pdf, geometry="poly_4326_fc")
        _gpd.to_parquet(f"{CENS_STAGING_PATH}/gpd_parquets/pc/{ladcd}.parquet")

    # Union all gpds to one single parquet; Does not provide require performance
    # pc_gpd_all = [
    #     gpd.read_parquet(lad_gpd)
    #     for lad_gpd in glob.glob(f"{CENS_STAGING_PATH}/gpd_parquets/pc/*.parquet")
    # ]
    # pc_gpd = gpd.GeoDataFrame(pd.concat(pc_gpd_all, ignore_index=False))

    spark.stop()
    # return a success df required for DBT model
    df = pd.DataFrame.from_dict([{"pc_poly_gpd_created", "success"}])
    return df



