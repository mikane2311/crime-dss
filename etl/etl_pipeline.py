# =====================================================
# ETL Pipeline - Crime DSS Los Angeles
# Script: etl_pipeline.py
# Description: Automated ETL pipeline for KPI refresh
#              and data quality checks
# =====================================================

import psycopg2
import logging
import sys
from datetime import datetime

# ─── Logging setup ────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log")
    ]
)
log = logging.getLogger(__name__)

# ─── Database config ──────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "crime_db",
    "user":     "crime_user",
    "password": "crime_pass"
}

# =====================================================
# STEP 1 : Connect to PostgreSQL
# =====================================================
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        log.error(f"Connection failed: {e}")
        sys.exit(1)

# =====================================================
# STEP 2 : Data quality checks
# =====================================================
def run_quality_checks(conn):
    log.info("Running data quality checks...")
    cursor = conn.cursor()

    # Check NULLs in fact_crime
    cursor.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE year IS NULL)        AS null_year,
            COUNT(*) FILTER (WHERE location_id IS NULL) AS null_location,
            COUNT(*) FILTER (WHERE crime_desc IS NULL)  AS null_crime_desc
        FROM fact_crime;
    """)
    row = cursor.fetchone()
    total, null_year, null_loc, null_desc = row
    log.info(f"Total rows     : {total:,}")
    log.info(f"NULL year      : {null_year}")
    log.info(f"NULL location  : {null_loc}")
    log.info(f"NULL crime_desc: {null_desc}")

    if null_year > 0 or null_loc > 0 or null_desc > 0:
        log.warning("NULLs detected in fact_crime — review ETL source data.")
    else:
        log.info("Quality check passed: no NULLs detected.")

    # Check duplicates
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT crime_id
            FROM fact_crime
            GROUP BY crime_id
            HAVING COUNT(*) > 1
        ) sub;
    """)
    duplicates = cursor.fetchone()[0]
    if duplicates > 0:
        log.warning(f"Duplicates detected: {duplicates} crime_id(s) appear more than once.")
    else:
        log.info("Quality check passed: no duplicates detected.")

    # Check orphan records
    cursor.execute("""
        SELECT COUNT(*) FROM fact_crime f
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_location l WHERE l.location_id = f.location_id
        );
    """)
    orphans = cursor.fetchone()[0]
    if orphans > 0:
        log.warning(f"Orphan records detected: {orphans} rows with unmatched location_id.")
    else:
        log.info("Quality check passed: no orphan records detected.")

    cursor.close()

# =====================================================
# STEP 3 : Refresh KPI via stored procedure
# =====================================================
def refresh_kpi(conn):
    log.info("Refreshing KPI audit log...")
    cursor = conn.cursor()
    try:
        cursor.execute("CALL refresh_kpi_log();")
        conn.commit()
        log.info("KPI log refreshed successfully.")
    except Exception as e:
        conn.rollback()
        log.error(f"KPI refresh failed: {e}")
    finally:
        cursor.close()

# =====================================================
# STEP 4 : Verify KPI results
# =====================================================
def verify_kpi(conn):
    log.info("Verifying KPI audit log...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT year, total_crimes, crime_rate, unemployment, refreshed_at
        FROM kpi_audit_log
        ORDER BY year;
    """)
    rows = cursor.fetchall()
    log.info("KPI Audit Log:")
    log.info(f"{'Year':<6} {'Total Crimes':<15} {'Crime Rate':<12} {'Unemployment':<14} {'Refreshed At'}")
    log.info("-" * 65)
    for r in rows:
        log.info(f"{r[0]:<6} {r[1]:<15,} {float(r[2]):<12.2f} {float(r[3]):<14.4f} {r[4]}")
    cursor.close()

# =====================================================
# MAIN
# =====================================================
def main():
    log.info("=" * 55)
    log.info("ETL Pipeline started")
    log.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    conn = get_connection()

    run_quality_checks(conn)
    refresh_kpi(conn)
    verify_kpi(conn)

    conn.close()
    log.info("ETL Pipeline completed successfully.")
    log.info("=" * 55)

if __name__ == "__main__":
    main()
