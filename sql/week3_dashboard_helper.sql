DROP VIEW IF EXISTS vw_crime_year_location;

CREATE VIEW vw_crime_year_location AS
SELECT
    f.year,
    l.area_name,
    COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l
    ON f.location_id = l.location_id
GROUP BY f.year, l.area_name
ORDER BY f.year, total_crimes DESC;
