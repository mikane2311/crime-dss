-- =====================================================
-- WEEK 4 - OPTIMIZED SQL SCRIPT
-- Project: Crime DSS - Los Angeles
-- =====================================================
-- Improvements:
--   1. Indexes on fact_crime (year, location_id, crime_desc, composite)
--   2. CTEs replacing nested subqueries (Q4, Q6)
--   3. NULL/duplicate/orphan detection
--   4. Stored procedure for KPI refresh
--   5. Performance verified with EXPLAIN ANALYZE
--
-- Performance results (fact_crime: 1,004,894 rows):
--   Before : Seq Scan, disk sort ~10MB/worker, ~1180ms
--   After  : Index Only Scan, disk sort ~2-4MB/worker
--   Gain   : Heap Fetches = 0 on idx_fact_crime_year
--   Note   : Q1 aggregates 1M rows, time incompressible
--            without partitioning (out of scope)
-- =====================================================


-- =====================================================
-- PART 1 : INDEXATION
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_fact_crime_year
    ON fact_crime(year);

CREATE INDEX IF NOT EXISTS idx_fact_crime_location_id
    ON fact_crime(location_id);

CREATE INDEX IF NOT EXISTS idx_fact_crime_crime_desc
    ON fact_crime(crime_desc);

CREATE INDEX IF NOT EXISTS idx_fact_crime_year_location
    ON fact_crime(year, location_id);

SELECT indexname, tablename, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;


-- =====================================================
-- PART 2 : NULL AND DUPLICATE DETECTION
-- =====================================================

-- NULLs in fact_crime (result: 0 NULLs on all key columns)
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(*) FILTER (WHERE year IS NULL)            AS null_year,
    COUNT(*) FILTER (WHERE location_id IS NULL)     AS null_location_id,
    COUNT(*) FILTER (WHERE crime_code IS NULL)      AS null_crime_code,
    COUNT(*) FILTER (WHERE crime_desc IS NULL)      AS null_crime_desc,
    COUNT(*) FILTER (WHERE lat IS NULL)             AS null_lat,
    COUNT(*) FILTER (WHERE lon IS NULL)             AS null_lon
FROM fact_crime;

-- NULLs in dimension tables
SELECT 'population'  AS table_name,
       COUNT(*) FILTER (WHERE population IS NULL) AS null_count
FROM population
UNION ALL
SELECT 'unemployment',
       COUNT(*) FILTER (WHERE unemployment_rate IS NULL)
FROM unemployment
UNION ALL
SELECT 'dim_location',
       COUNT(*) FILTER (WHERE area_name IS NULL)
FROM dim_location;

-- Duplicates in fact_crime (expected: 0 rows)
SELECT crime_id, COUNT(*) AS occurrences
FROM fact_crime
GROUP BY crime_id
HAVING COUNT(*) > 1;

-- Orphan records (expected: 0)
SELECT COUNT(*) AS orphan_crimes
FROM fact_crime f
WHERE NOT EXISTS (
    SELECT 1 FROM dim_location l WHERE l.location_id = f.location_id
);


-- =====================================================
-- PART 3 : OPTIMIZED QUERIES WITH CTEs
-- =====================================================

-- Q1: Crimes per year with population, unemployment, crime rate
-- Optimization: benefits from idx_fact_crime_year
SELECT
    f.year,
    COUNT(*)                                                        AS total_crimes,
    p.population,
    u.unemployment_rate,
    ROUND((COUNT(*)::numeric / p.population) * 100000, 2)          AS crime_rate_per_100k
FROM fact_crime f
JOIN population   p ON f.year = p.year
JOIN unemployment u ON f.year = u.year
GROUP BY f.year, p.population, u.unemployment_rate
ORDER BY f.year;

-- Q2: Top 5 most affected areas
-- Optimization: benefits from idx_fact_crime_location_id
SELECT
    l.area_name,
    COUNT(*)                                                        AS total_crimes
FROM fact_crime f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.area_name
ORDER BY total_crimes DESC
LIMIT 5;

-- Q3: Top 5 crime types
-- Optimization: benefits from idx_fact_crime_crime_desc
SELECT
    crime_desc,
    COUNT(*)                                                        AS total_crimes
FROM fact_crime
GROUP BY crime_desc
ORDER BY total_crimes DESC
LIMIT 5;

-- Q4 (optimized): Areas above average crime count
-- Before: nested subquery in HAVING
-- After : CTE computes avg once, reused in main query
WITH area_counts AS (
    SELECT location_id, COUNT(*) AS total_crimes
    FROM fact_crime
    GROUP BY location_id
),
avg_crimes AS (
    SELECT AVG(total_crimes) AS avg_count
    FROM area_counts
)
SELECT
    l.area_name,
    ac.total_crimes
FROM area_counts ac
JOIN dim_location l  ON ac.location_id = l.location_id
JOIN avg_crimes  avg ON ac.total_crimes > avg.avg_count
ORDER BY ac.total_crimes DESC;

-- Q5: Crime intensity level by year
SELECT
    year,
    total_crimes,
    crime_rate_per_100k,
    CASE
        WHEN crime_rate_per_100k >= 6000 THEN 'High'
        WHEN crime_rate_per_100k >= 5000 THEN 'Medium'
        ELSE 'Low'
    END                                                             AS crime_intensity_level
FROM vw_crime_yearly_kpi
ORDER BY year;

-- Q6 (optimized): Annual variation with LAG()
-- Before: LAG() called twice in SELECT
-- After : CTE computes LAG() once, reused in outer query
WITH yearly_data AS (
    SELECT
        year,
        total_crimes,
        LAG(total_crimes) OVER (ORDER BY year) AS prev_year_crimes
    FROM vw_crime_yearly_kpi
)
SELECT
    year,
    total_crimes,
    prev_year_crimes,
    ROUND(
        (total_crimes - prev_year_crimes)::numeric
        / NULLIF(prev_year_crimes, 0) * 100, 2
    )                                                               AS annual_variation_percent
FROM yearly_data
ORDER BY year;

-- Q7: Crimes by area and year
-- Optimization: benefits from idx_fact_crime_year_location (composite)
SELECT
    f.year,
    l.area_name,
    COUNT(*)                                                        AS total_crimes
FROM fact_crime f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY f.year, l.area_name
ORDER BY f.year, total_crimes DESC;

-- Q8 (new): Top crime type per area — CTE + RANK()
WITH crime_area_type AS (
    SELECT
        l.area_name,
        f.crime_desc,
        COUNT(*) AS total_crimes
    FROM fact_crime f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.area_name, f.crime_desc
),
ranked AS (
    SELECT
        area_name,
        crime_desc,
        total_crimes,
        RANK() OVER (PARTITION BY area_name ORDER BY total_crimes DESC) AS rnk
    FROM crime_area_type
)
SELECT area_name, crime_desc, total_crimes
FROM ranked
WHERE rnk = 1
ORDER BY total_crimes DESC;


-- =====================================================
-- PART 4 : STORED PROCEDURE FOR KPI REFRESH
-- =====================================================

CREATE TABLE IF NOT EXISTS kpi_audit_log (
    log_id       SERIAL PRIMARY KEY,
    refreshed_at TIMESTAMP DEFAULT NOW(),
    year         INT,
    total_crimes BIGINT,
    crime_rate   NUMERIC(10,2),
    unemployment NUMERIC(10,4)
);

CREATE OR REPLACE PROCEDURE refresh_kpi_log()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM kpi_audit_log;
    INSERT INTO kpi_audit_log (year, total_crimes, crime_rate, unemployment)
    SELECT year, total_crimes, crime_rate_per_100k, unemployment_rate
    FROM vw_crime_yearly_kpi;
    RAISE NOTICE 'KPI log refreshed at %', NOW();
END;
$$;

CALL refresh_kpi_log();

SELECT * FROM kpi_audit_log ORDER BY year;


-- =====================================================
-- PART 5 : PERFORMANCE VERIFICATION
-- =====================================================
-- After indexes:
--   Scan  : Seq Scan → Parallel Index Only Scan
--   Heap Fetches = 0
--   Disk sort : ~10MB/worker → ~2-4MB/worker

EXPLAIN ANALYZE
SELECT
    f.year,
    COUNT(*),
    p.population,
    u.unemployment_rate,
    ROUND((COUNT(*)::numeric / p.population) * 100000, 2) AS crime_rate_per_100k
FROM fact_crime f
JOIN population   p ON f.year = p.year
JOIN unemployment u ON f.year = u.year
GROUP BY f.year, p.population, u.unemployment_rate
ORDER BY f.year;
