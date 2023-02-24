{{ config(
    materialized="table"
) }}


WITH wards AS (
	SELECT DISTINCT oacd, wdcd
	FROM {{ ref('area_mapping_cens') }}
),
withOa AS (
	SELECT DISTINCT lat, lon, oa21cd AS oacd, lad22cd AS ladcd
	FROM {{ ref('crime_point_index_w_oa') }}
),
withCrimeAgg AS (
    SELECT
        aggRaw.*,
        withOa.oacd AS oacd,
        withOa.ladcd AS ladcd,
        wards.wdcd
    FROM
    {{ ref('crime_agg_raw') }} aggRaw LEFT OUTER JOIN
    withOa ON withOa.lat = aggRaw.lat AND withOa.lon = aggRaw.lon
    LEFT OUTER JOIN wards ON withOa.oacd = wards.oacd
)
SELECT
    year,
	oacd,
	ladcd,
	wdcd,
	crime_type_cd,
	sum(crime_count) AS crime_count
FROM withCrimeAgg
GROUP BY 1, 2, 3, 4, 5

