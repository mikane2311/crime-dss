-- =====================
-- DIMENSION: time
-- =====================
CREATE TABLE dim_time (
    year INT PRIMARY KEY
);

-- =====================
-- DIMENSION: location
-- =====================
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    area_code INT UNIQUE,
    area_name TEXT
);

-- =====================
-- DIMENSION: population
-- =====================
CREATE TABLE population (
    year INT PRIMARY KEY,
    population BIGINT,
    FOREIGN KEY (year) REFERENCES dim_time(year)
);

-- =====================
-- DIMENSION: unemployment
-- =====================
CREATE TABLE unemployment (
    year INT PRIMARY KEY,
    unemployment_rate FLOAT,
    FOREIGN KEY (year) REFERENCES dim_time(year)
);

-- =====================
-- FACT TABLE
-- =====================
CREATE TABLE fact_crime (
    crime_id BIGINT PRIMARY KEY,
    year INT,
    location_id INT,
    crime_code INT,
    crime_desc TEXT,
    lat FLOAT,
    lon FLOAT,

    FOREIGN KEY (year) REFERENCES dim_time(year),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);
