from utils.spark_utils import get_spatial_spark_session
from utils.params import CENS_STAGING_PATH
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter
from pyspark.sql import functions as f
import glob
import pandas as pd
import os
import geopandas as gpd

# geopandas.tools.reverse_geocode explore this for better option may be? bulk points works?
# Join with house prices data to get the post code first. Then go for geocode

# def model(dbt, session):
#
#     # DataFrame representing an upstream model
#     upstream_model = dbt.ref("upstream_model_name")
#
#     # DataFrame representing an upstream source
#     upstream_source = dbt.source("upstream_source_name", "table_name")


# Below code needs to be used to create the geoFrame
# _wards_pdf = spark.read.parquet(f"/mnt/c/wsl_transfers/cens_staging/geo_parquets/_wards/").limit(10)\
# .withColumn("poly_4326_fc",f.expr("ST_FlipCoordinates(poly_4326)"))\
# .toPandas().set_index("WD22CD")
# _wards_gpd = gpd.GeoDataFrame(_wards_pdf[["poly_4326_fc"]], geometry="poly_4326_fc")


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    spark = get_spatial_spark_session("dbtSpark")
    shape_files = glob.glob(f"{CENS_STAGING_PATH}/geo_shape_files/*")
    for shape_file in shape_files:
        file_id = os.path.basename(shape_file).split('.')[0]
        file_id = file_id.replace('shape_file_','')
        print(f"processing {file_id}")
        shapefileInputLocation = shape_file
        spatialRDD = ShapefileReader \
            .readToGeometryRDD(spark, shapefileInputLocation)
        dfRaw = Adapter.toDf(spatialRDD, spark)
        dfRaw \
            .withColumn("poly_4326_fc",
                        f.expr("ST_FlipCoordinates(ST_Transform(geometry, 'epsg:27700','epsg:4326'))")) \
            .repartition(4, "poly_4326_fc") \
            .drop("geometry")\
            .write.mode("overwrite")\
            .parquet(f"{CENS_STAGING_PATH}/geo_parquets/{file_id}/")
        print(f"{file_id}: completed")
    # Convert to gpd; spark read - pandas with index - gpd
    geo_keys = {
        "oa": "OA21CD",
        # "ward": "WD22CD",
        # "lad": "LAD22CD",
        # "lsoa": "LSOA21CD",
        # "msoa": "MSOA21CD"
    }
    for x in geo_keys.items():
        area_lvl = x[0]
        area_col = x[1]
        _pdf = spark.read.parquet(f"{CENS_STAGING_PATH}/geo_parquets/{area_lvl}/") \
            .selectExpr(f"{area_col} as area_cd", "poly_4326_fc") \
            .toPandas().set_index("area_cd")

        _gpd = gpd.GeoDataFrame(_pdf, geometry="poly_4326_fc")
        _gpd.to_parquet(f"{CENS_STAGING_PATH}/gpd_parquets/{area_lvl}_gpd.parquet")

    spark.stop()
    # return a success df required for DBT model
    df = pd.DataFrame.from_dict([{"shapeFilesWritten", "success"}])
    return df



