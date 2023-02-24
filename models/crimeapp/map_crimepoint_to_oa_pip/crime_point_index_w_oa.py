from utils.params import CENS_STAGING_PATH
from utils.dbutils import pl_df_from_query, sdf_df_from_query
from utils.spark_utils import get_spatial_spark_session
from pyspark.sql import functions as f
import pandas as pd
# Long running


def model(dbt, session):
    dbt.config(
        materialized="table"
    )
    # Below dbt.ref returns a pandas df. Dont have to specify connection details at all
    crime_point_index_pdf = dbt.ref("pip_join_point_w_polygon")
    pdf = pd.read_parquet(f"{CENS_STAGING_PATH}/temp/crime_point_in_oa_poly/")
    return pdf[~pdf['lat'].isna()]
