-- postcodes -> postcode lookups -> census postcode lookups
--https://geoportal.statistics.gov.uk/search?collection=Dataset&sort=-created&tags=all(CEN2021_LUP_PCD_OA_LSOA_MSOA_LAD)
{{ config(
    materialized="table"
) }}

select * from read_csv('/mnt/c/wsl_transfers/cens_staging/pc_to_oa_mapping/*.csv', auto_detect=True)