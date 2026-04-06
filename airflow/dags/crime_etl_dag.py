# =====================================================
# Airflow DAG - Crime DSS Los Angeles
# File: crime_etl_dag.py
# Description: Orchestrates the ETL pipeline tasks
#              Schedule: every day at 6:00 AM
# =====================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging

log = logging.getLogger(__name__)

# ─── Database config ──────────────────────────────
DB_CONFIG = {
    "host":     "host.docker.internal",  # reaches localhost from inside Docker
    "port":     5432,
    "dbname":   "crime_db",
    "user":     "crime_user",
    "password": "crime_pass"
}

# ─── Default DAG args ─────────────────────────────
default_args = {
    "owner":            "crime_dss",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# =====================================================
# TASK FUNCTIONS
# =====================================================

def task_check_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    log.info("PostgreSQL connection successful.")
    conn.close()

def task_quality_checks():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE year IS NULL)        AS null_year,
            COUNT(*) FILTER (WHERE location_id IS NULL) AS null_location,
            COUNT(*) FILTER (WHERE crime_desc IS NULL)  AS null_crime_desc
        FROM fact_crime;
    """)
    null_year, null_loc, null_desc = cursor.fetchone()

    if null_year > 0 or null_loc > 0 or null_desc > 0:
        raise ValueError(f"NULLs detected: year={null_year}, location={null_loc}, desc={null_desc}")
    log.info("Quality check passed: no NULLs.")

    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT crime_id FROM fact_crime
            GROUP BY crime_id HAVING COUNT(*) > 1
        ) sub;
    """)
    duplicates = cursor.fetchone()[0]
    if duplicates > 0:
        raise ValueError(f"Duplicates detected: {duplicates} rows.")
    log.info("Quality check passed: no duplicates.")

    cursor.execute("""
        SELECT COUNT(*) FROM fact_crime f
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_location l WHERE l.location_id = f.location_id
        );
    """)
    orphans = cursor.fetchone()[0]
    if orphans > 0:
        raise ValueError(f"Orphan records detected: {orphans} rows.")
    log.info("Quality check passed: no orphans.")

    cursor.close()
    conn.close()

def task_refresh_kpi():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("CALL refresh_kpi_log();")
    conn.commit()
    log.info("KPI log refreshed successfully.")
    cursor.close()
    conn.close()

def task_verify_kpi():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT year, total_crimes, crime_rate, unemployment
        FROM kpi_audit_log
        ORDER BY year;
    """)
    rows = cursor.fetchall()
    for r in rows:
        log.info(f"Year={r[0]} | Crimes={r[1]:,} | Rate={r[2]} | Unemployment={r[3]}")
    cursor.close()
    conn.close()

# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id="crime_dss_etl_pipeline",
    description="Daily ETL pipeline for Crime DSS Los Angeles",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # every day at 06:00
    catchup=False,
    tags=["crime", "etl", "dss"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_connection = PythonOperator(
        task_id="check_connection",
        python_callable=task_check_connection,
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
    )

    refresh_kpi = PythonOperator(
        task_id="refresh_kpi",
        python_callable=task_refresh_kpi,
    )

    verify_kpi = PythonOperator(
        task_id="verify_kpi",
        python_callable=task_verify_kpi,
    )

    end = EmptyOperator(task_id="end")

    # ─── Pipeline order ───────────────────────────
    start >> check_connection >> quality_checks >> refresh_kpi >> verify_kpi >> end
