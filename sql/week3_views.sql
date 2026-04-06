-- =====================================================
-- WEEK 3 - SQL VIEWS
-- Project: Crime DSS - Los Angeles
-- =====================================================

-- -----------------------------------------------------
-- VIEW 1: yearly KPI view
-- Purpose:
-- gives one row per year with:
-- total crimes, population, unemployment, crime rate
-- -----------------------------------------------------
DROP VIEW IF EXISTS vw_crime_yearly_kpi;

CREATE VIEW vw_crime_yearly_kpi AS
SELECT
    f.year,
    COUNT(*) AS total_crimes,
    p.population,
    u.unemployment_rate,
    ROUND((COUNT(*)::numeric / p.population) * 100000, 2) AS crime_rate_per_100k
FROM fact_crime f
JOIN population p
    ON f.year = p.year
JOIN unemployment u
    ON f.year = u.year
GROUP BY f.year, p.population, u.unemployment_rate
ORDER BY f.year;

-- -----------------------------------------------------
-- VIEW 2: crimes by location
-- Purpose:
-- gives total crimes for each Los Angeles area
-- -----------------------------------------------------
DROP VIEW IF EXISTS vw_crime_by_location;

CREATE VIEW vw_crime_by_location AS
SELECT
    l.location_id,
    l.area_code,
    l.area_name,
    COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l
    ON f.location_id = l.location_id
GROUP BY l.location_id, l.area_code, l.area_name
ORDER BY total_crimes DESC;

-- -----------------------------------------------------
-- VIEW 3: crimes by type
-- Purpose:
-- gives crime count by crime type
-- -----------------------------------------------------
DROP VIEW IF EXISTS vw_crime_by_type;

CREATE VIEW vw_crime_by_type AS
SELECT
    crime_code,
    crime_desc,
    COUNT(*) AS total_crimes
FROM fact_crime
GROUP BY crime_code, crime_desc
ORDER BY total_crimes DESC;
