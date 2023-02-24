--https://geoportal.statistics.gov.uk/documents/ons::output-area-to-ward-to-local-authority-district-december-2021-lookup-in-england-and-wales-1/about
--lookups-bestfitlookups - outputareas - census - oa to ward

{{ config(
    materialized="table"
) }}

select * from read_csv('/mnt/c/wsl_transfers/cens_staging/oa_to_ward_mapping/*.csv', auto_detect=True)