-- =====================================================
-- WEEK 3 - ADVANCED ANALYTICAL SQL
-- Project: Crime DSS - Los Angeles
-- =====================================================

-- -----------------------------------------------------
-- Q1: Strategic KPI
-- Crimes per year with population, unemployment and crime rate
-- Uses a multi-table join
-- -----------------------------------------------------
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
-- Q2: Strategic KPI
-- Top 5 most affected areas
-- Uses GROUP BY and ORDER BY
-- -----------------------------------------------------
SELECT
    l.area_name,
    COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l
    ON f.location_id = l.location_id
GROUP BY l.area_name
ORDER BY total_crimes DESC
LIMIT 5;

-- -----------------------------------------------------
-- Q3: Strategic KPI
-- Top 5 crime types
-- Uses aggregation and ORDER BY
-- -----------------------------------------------------
SELECT
    crime_desc,
    COUNT(*) AS total_crimes
FROM fact_crime
GROUP BY crime_desc
ORDER BY total_crimes DESC
LIMIT 5;

-- -----------------------------------------------------
-- Q4: Strategic KPI
-- Areas having more crimes than the average area
-- Uses subquery + HAVING
-- -----------------------------------------------------
SELECT
    l.area_name,
    COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l
    ON f.location_id = l.location_id
GROUP BY l.area_name
HAVING COUNT(*) > (
    SELECT AVG(area_crime_count)
    FROM (
        SELECT COUNT(*) AS area_crime_count
        FROM fact_crime
        GROUP BY location_id
    ) sub
)
ORDER BY total_crimes DESC;

-- -----------------------------------------------------
-- Q5: Strategic KPI
-- Crime intensity level by year
-- Uses CASE WHEN
-- -----------------------------------------------------
SELECT
    year,
    total_crimes,
    crime_rate_per_100k,
    CASE
        WHEN crime_rate_per_100k >= 6000 THEN 'High'
        WHEN crime_rate_per_100k >= 5000 THEN 'Medium'
        ELSE 'Low'
    END AS crime_intensity_level
FROM vw_crime_yearly_kpi
ORDER BY year;

-- -----------------------------------------------------
-- Q6: Operational KPI
-- Annual variation of crimes in percentage
-- Uses window function LAG
-- -----------------------------------------------------
SELECT
    year,
    total_crimes,
    LAG(total_crimes) OVER (ORDER BY year) AS previous_year_crimes,
    ROUND(
        (
            (total_crimes - LAG(total_crimes) OVER (ORDER BY year))::numeric
            / NULLIF(LAG(total_crimes) OVER (ORDER BY year), 0)
        ) * 100,
        2
    ) AS annual_variation_percent
FROM vw_crime_yearly_kpi
ORDER BY year;

-- -----------------------------------------------------
-- Q7: Operational KPI
-- Number of crimes by area and by year
-- Useful for local monitoring
-- -----------------------------------------------------
SELECT
    f.year,
    l.area_name,
    COUNT(*) AS total_crimes
FROM fact_crime f
JOIN dim_location l
    ON f.location_id = l.location_id
GROUP BY f.year, l.area_name
ORDER BY f.year, total_crimes DESC;
