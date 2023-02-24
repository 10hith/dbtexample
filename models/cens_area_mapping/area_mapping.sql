{{ config(
    materialized="table"
) }}

with
wards AS
(   select
        distinct oa21cd as oacd, wd22cd, wd22nm
    from {{ ref('oa_to_ward_mapping') }}
    )
select distinct
	oa21cd as oacd,
	lsoa21cd as lsoacd,
	wards.wd22cd as wdcd,
	wards.wd22nm as wdnm,
	ltla22cd as ladcd,
	ltla22nm as ladnm,
	utla22cd as utlacd,
	utla22nm as utlanm
from {{ ('pc_to_oa_mapping') }} mapping
left outer join wards on
mapping.oa21cd = wards.oacd