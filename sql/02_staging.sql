-- A staging table to load fact rows quickly from CSV
DROP TABLE IF EXISTS stg_fact_crime;

CREATE TABLE stg_fact_crime (
    crime_id BIGINT,
    year INT,
    area_code INT,
    area_name TEXT,
    crime_code INT,
    crime_desc TEXT,
    lat FLOAT,
    lon FLOAT
);
