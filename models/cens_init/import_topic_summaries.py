import pandas as pd
import glob
import duckdb
import os
# from utils.params import DATA_PATH

creating_lkp_view = """
create table area_lkp_nm_cd_lvl AS (
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts001 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts002 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts003 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts004 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts005 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts006 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts007a union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts008 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts011 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts012 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts013 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts015 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts016 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts017 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts018 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts019 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts021 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts022 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts023 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts024 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts025 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts026 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts027 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts028 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts029 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts030 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts031 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts037 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts044 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts045 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts048 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts051 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts060 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts062 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts063 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts064 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts066 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts067 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts068 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts077 union all
SELECT distinct geography as area_nm, "geography code" as area_cd, area_lvl from ts078
)
"""


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["polars"]
    )
    files = glob.glob(f"/mnt/c/wsl_transfers/cens_staging/cens_topic_summaries_parquet/*.parquet")
    con = duckdb.connect("/mnt/c/wsl_transfers/cens.db")

    for file in files:
        table_name = os.path.basename(file).split('.')[0]
        con.execute(f"""
        create or replace table {table_name} as (
        with raw_tbl as (select * from read_parquet('{file}'))
        select
            case 
            when filename like '%-ctry.csv%' then 'ctry'
            when filename like '%-rgn.csv%' then 'rgn'
            when filename like '%-ulta.csv%' then 'ulta'
            when filename like '%-ltla.csv%' then 'lad'
            when filename like '%-ward.csv%' then 'ward'
            when filename like '%-msoa.csv%' then 'msoa'
            when filename like '%-msoa.csv%' then 'msoa'
            when filename like '%-lsoa.csv%' then 'lsoa'
            when filename like '%-oa.csv%' then 'oa'
            else 'na'
            end AS area_lvl,
            *
        from raw_tbl)
        """)
    con.execute(creating_lkp_view)
    con.close()
    df = pd.DataFrame.from_dict([{"tableLoad", "success"}])
    return df