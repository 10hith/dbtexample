{{ config(
    materialized="incremental"
) }}
--dbt run --model crime_agg_raw --vars '{"source_table": "crime_raw_2013", "year_str": "2013"}'
with crime_agg_raw_types as (
    SELECT
        {{var('year_str')}} as year,
        lat,
        lon,
        lsoa_cd,
        CASE
            WHEN crime_type='Violence and sexual offences' THEN 0
            WHEN crime_type='Anti-social behaviour' THEN 1
            WHEN crime_type='Public order' THEN 2
            WHEN crime_type='Criminal damage and arson' THEN 3
            WHEN crime_type='Other theft' THEN 4
            WHEN crime_type='Vehicle crime' THEN 5
            WHEN crime_type='Shoplifting' THEN 6
            WHEN crime_type='Burglary' THEN 7
            WHEN crime_type='Drugs' THEN 8
            WHEN crime_type='Other crime' THEN 9
            WHEN crime_type='Bicycle theft' THEN 10
            WHEN crime_type='Theft from the person' THEN 11
            WHEN crime_type='Robbery' THEN 12
            WHEN crime_type='Possession of weapons' THEN 13
            ELSE -1
        END AS crime_type_cd,
        COUNT(*) as crime_count
    FROM {{var('source_table')}}
    GROUP BY 1,2,3,4,5
),
crime_agg_raw_99 as (
    SELECT
        {{var('year_str')}} as year,
        lat,
        lon,
        lsoa_cd,
        99 as crime_type_cd,
        COUNT(*) as crime_count
    FROM {{var('source_table')}}
    GROUP BY 1,2,3,4,5
),
just_ref as (
    SELECT "ref" FROM {{ ref('crime_csv_landing') }}
)
crime_agg_raw as (
    SELECT * FROM crime_agg_raw_types
    UNION
    SELECT * FROM crime_agg_raw_99
)
SELECT * FROM crime_agg_raw
