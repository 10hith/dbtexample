{{ config(
    materialized="table",
    post_hook=[
      "alter table {{ this }} rename to {{var('table_name')}} "
    ]
) }}

{% set schema = {
    'crime_id': 'VARCHAR',
    'mnth': 'VARCHAR',
    'reported_by': 'VARCHAR',
    'fall_within': 'VARCHAR',
    'lon': 'DOUBLE',
    'lat': 'DOUBLE',
    'location': 'VARCHAR',
    'lsoa_cd': 'VARCHAR',
    'lsoa_name': 'VARCHAR',
    'crime_type': 'VARCHAR',
    'outcome': 'VARCHAR',
    'context': 'VARCHAR'
} %}

--dbt run --model crime_csv_landing --vars '{"glob_pattern": "/mnt/c/Users/lohith/Downloads/crime_data_Archive_IMP/2015-12/2013-*/*street.csv", "table_name": "crime_raw_2013"}'

select *
from read_csv(
    '{{var("glob_pattern")}}',
    header=True, HIVE_PARTITIONING=1, columns={{schema}}
    )

