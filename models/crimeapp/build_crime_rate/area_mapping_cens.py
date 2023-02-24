from utils.dbutils import pl_df_from_query


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
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
    return area_mapping.to_pandas()