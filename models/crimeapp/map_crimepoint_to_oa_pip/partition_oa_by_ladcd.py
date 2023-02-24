from utils.params import CENS_STAGING_PATH
from utils.dbutils import pl_df_from_query, sdf_df_from_query
from utils.spark_utils import get_spatial_spark_session
from pyspark.sql import functions as f
import pandas as pd
# Long running


def model(dbt, session):
    dbt.config(
        materialized="view"
    )

    # upstream_source = dbt.source("main", "oa_to_ward_mapping")
    spark = get_spatial_spark_session("partitionOaByLadcd")
    query = "select distinct OA21CD as oa21cd, LTLA22CD as lad22cd from oa_to_ward_mapping"
    oa_to_lad = sdf_df_from_query("crimeapp_init", query, spark)

    # Spread the geo across partitions for easier read
    oa_spark_geo = spark.read.parquet(f"{CENS_STAGING_PATH}/geo_parquets/oa/")\
        .withColumn("poly_4326", f.expr(
            "ST_FlipCoordinates(poly_4326_fc)"
        ))

    oa_spark_geo \
        .join(
            f.broadcast(oa_to_lad),
            on="oa21cd",
            how="left"
        ) \
        .selectExpr(
            "oa21cd",
            "poly_4326",
            "lad22cd",
        ) \
        .repartition(4, "poly_4326") \
        .write.mode("overwrite") \
        .partitionBy("lad22cd") \
        .parquet(f"{CENS_STAGING_PATH}/temp/oa_geo_lad_partition/")

    op_dict = dict(oa_spark_geo.dtypes)
    op_dict['loaded'] = "success"

    spark.stop()
    return pd.DataFrame.from_dict([op_dict])
