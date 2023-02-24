{{ config(
    materialized="table"
) }}

with distinct_crime_points as (
	select distinct lat, lon, lsoa_cd as lsoa11cd from crime_agg_raw
	)
select
	points.*,
	lsoa.lsoa21cd,
	lsoa.lad22cd
from distinct_crime_points as points
left outer join {{ ref('lsoa11_to_21') }} as lsoa
on points.lsoa11cd = lsoa.lsoa11cd