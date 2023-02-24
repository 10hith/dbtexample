{%- set source_relation = adapter.get_relation(
      database="main",
      schema="PDF_ALL_AREA_CRIME_RAW",
      identifier="all area crime") -%}

{{ log("Source Relation: " ~ source_relation, info=true) }}