from utils.dbutils import pl_df_from_query
import polars as pl


def gen_area_key(name_col: pl.col):
    area_key = name_col.str.to_lowercase()\
        .str.replace_all(r"[&,-,(,),:,']", '')\
        .str.replace_all(r'\s{2}', ' ')\
        .str.replace_all(r'\s{1}', '-')
    return area_key


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    area_mapping = dbt.ref("area_mapping")
    area_mapping_query = """
    SELECT
    	*
    FROM
    	area_mapping
    WHERE
    	oacd IS NOT NULL
    	AND wdnm IS NOT NULL
    ORDER BY
    	oacd
    """
    area_mapping = pl_df_from_query("cens", area_mapping_query)

    with pl.StringCache():
        ward_df = area_mapping.select([
            pl.lit('ward').alias("area_lvl"),
            pl.col("wdcd").alias("area_cd"),
            pl.col("wdnm").alias("area_nm")
        ]) \
            .unique()
        lad_df = area_mapping.select([
            pl.lit('lad').alias("area_lvl"),
            pl.col("ladcd").alias("area_cd"),
            pl.col("ladnm").alias("area_nm")
        ]) \
            .unique()
        district_df = area_mapping.select([
            pl.lit('district').alias("area_lvl"),
            pl.col("pcd_dist").alias("area_cd"),
            pl.col("pcd_dist").alias("area_nm")
        ]) \
            .unique()

        area_keys = pl.concat([ward_df, lad_df, district_df], rechunk=True) \
            .with_columns([
                gen_area_key(pl.col('area_nm').alias("area_key")),
                pl.col('area_lvl').cast(pl.Categorical)
            ])

    return area_keys.to_pandas()