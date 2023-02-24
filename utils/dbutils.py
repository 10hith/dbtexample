import polars as pl
import duckdb

# crimeCon = duckdb.connect("/home/basal/live_datasets/crimeapp_init.db")
# censCon = duckdb.connect("/home/basal/live_datasets/cens.db")

DATA_PATH = "/home/basal/live_datasets/processed"


def pl_df_from_query(db_name, query):
    """
    Create polars df from db query
    """
    con = duckdb.connect(f"{DATA_PATH}/{db_name}.db")
    pl_df = con.execute(query).pl()
    con.close()
    return pl_df


def sdf_df_from_query(db_name, query, spark):
    """
    Create spark df from db connection
    """
    pl_df = pl_df_from_query(db_name, query)
    return spark.createDataFrame(pl_df.to_dicts())


def pdf_from_query(db_name, query):
    """
    Create polars df from db query
    """
    con = duckdb.connect(f"{DATA_PATH}/{db_name}.db")
    pdf = con.execute(query).df()
    con.close()
    return pdf