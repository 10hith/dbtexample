from utils.params import CENS_STAGING_PATH
from utils.dbutils import pl_df_from_query, sdf_df_from_query
from utils.spark_utils import get_spatial_spark_session
from pyspark.sql import functions as f
import pandas as pd
from utils.dbutils import pdf_from_query
import glob
import geopandas as gpd


def model(dbt, session):
    dbt.config(
        materialized="table"
    )
    # Define upstream
    _ = dbt.ref("crime_point_index_w_oa")
    _ = dbt.ref("gpd_for_pc")

    # Create complete pc_gpd
    pc_gpd_all = [
        gpd.read_parquet(lad_gpd)
        for lad_gpd in glob.glob(f"{CENS_STAGING_PATH}/gpd_parquets/pc/*.parquet")
    ]
    pc_gpd = gpd.GeoDataFrame(pd.concat(pc_gpd_all, ignore_index=False))
    # Read crimePointIndex
    query = """
    SELECT * FROM crime_point_index_w_oa 
    WHERE lat IS NOT NULL OR 
        lon IS NOT NULL
    """
    crime_point_index = pdf_from_query("crimeapp", query)
    # Create Point geometry
    crime_point_index_gpd = gpd.GeoDataFrame(
        crime_point_index, geometry=gpd.points_from_xy(crime_point_index.lon, crime_point_index.lat)
    )
    # Spatial join crime_point with postcode
    crime_point_index_gpd_w_pc = pc_gpd.sjoin(
        crime_point_index_gpd,
        how="inner",
        predicate="contains"
    )
    crime_point_index_gpd_w_pc.index.name = 'pcd'
    return crime_point_index_gpd_w_pc.reset_index()\
        [['pcd', 'oa21cd', 'lad22cd', 'lat', 'lon', 'lsoa11cd']]
