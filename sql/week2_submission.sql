-- =====================================================
-- WEEK 2 SUBMISSION
-- Project: Crime DSS - Los Angeles
-- DBMS: PostgreSQL running in Docker
-- Container setup used:
-- docker run -d --name crime-postgres \
--   -e POSTGRES_USER=crime_user \
--   -e POSTGRES_PASSWORD=crime_pass \
--   -e POSTGRES_DB=crime_db \
--   -p 5432:5432 postgres:16
-- =====================================================

-- Database note:
-- CREATE DATABASE crime_db;
-- In practice, the database was initialized through Docker environment variables.

DROP TABLE IF EXISTS fact_crime CASCADE;
DROP TABLE IF EXISTS stg_fact_crime CASCADE;
DROP TABLE IF EXISTS population CASCADE;
DROP TABLE IF EXISTS unemployment CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_time (
    year INT PRIMARY KEY
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    area_code INT UNIQUE NOT NULL,
    area_name TEXT NOT NULL
);

CREATE TABLE population (
    year INT PRIMARY KEY,
    population BIGINT NOT NULL,
    FOREIGN KEY (year) REFERENCES dim_time(year)
);

CREATE TABLE unemployment (
    year INT PRIMARY KEY,
    unemployment_rate FLOAT NOT NULL,
    FOREIGN KEY (year) REFERENCES dim_time(year)
);

CREATE TABLE fact_crime (
    crime_id BIGINT PRIMARY KEY,
    year INT NOT NULL,
    location_id INT NOT NULL,
    crime_code INT,
    crime_desc TEXT,
    lat FLOAT,
    lon FLOAT,
    FOREIGN KEY (year) REFERENCES dim_time(year),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

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

CREATE INDEX idx_fact_crime_year ON fact_crime(year);
CREATE INDEX idx_fact_crime_location ON fact_crime(location_id);

-- =====================================================
-- 5 QUERIES DEMANDEES
-- =====================================================

-- Q1: SELECT simple
-- Purpose: display a sample of loaded crime data
SELECT * FROM fact_crime
LIMIT 10;

-- Q2: SELECT with WHERE
-- Purpose: filter crime records for a specific year
SELECT * FROM fact_crime
WHERE year = 2022
LIMIT 10;

-- Q3: GROUP BY
-- Purpose: count total crimes by year
SELECT year, COUNT(*) AS total_crimes
FROM fact_crime
GROUP BY year
ORDER BY year;

-- Q4: Join between at least 2 tables
-- Purpose: count crimes by area using the location dimension
SELECT l.area_name, COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.area_name
ORDER BY total_crimes DESC;

-- Q5: Aggregation with business meaning
-- Purpose: compute crime rate per 100k inhabitants
SELECT
    f.year,
    COUNT(*) AS total_crimes,
    p.population,
    ROUND((COUNT(*)::numeric / p.population) * 100000, 2) AS crime_rate_per_100k
FROM fact_crime f
JOIN population p ON f.year = p.year
GROUP BY f.year, p.population
ORDER BY f.year;
