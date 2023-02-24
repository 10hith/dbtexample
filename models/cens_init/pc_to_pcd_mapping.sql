--https://geoportal.statistics.gov.uk/datasets/postcode-to-postcode-sector-to-postcode-district-to-postcode-area-to-2021-output-area-august-2022-lookup-in-england-and-wales/about
{{ config(
    materialized="table"
) }}

with table_ as (
    select * from read_csv(
        '/mnt/c/wsl_transfers/cens_staging/pc_to_pcd_mapping/*.csv',
        auto_detect=True,
        header=True
        )
)
select * from table_