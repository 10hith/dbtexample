from utils.dbutils import pl_df_from_query


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    pop_query = """
    SELECT
    	DISTINCT area_lvl,
    	"geography code" AS area_cd,
    	"Sex: All persons; measures: Value" AS pop
    FROM
    	ts008
    """
    pop = pl_df_from_query("cens", pop_query)

    return pop.to_pandas()