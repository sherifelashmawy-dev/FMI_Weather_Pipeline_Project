"""
FMI Weather Data Processing DAG
================================
This DAG runs daily at 1:00 AM to process weather data from BigQuery.

Schedule: Daily at 1:00 AM
Tasks:
1. Get processing date (yesterday)
2. Process raw observations (deduplicate, quality check)
3. Create daily summary statistics
4. Generate quality report
5. Update long-term station tables (5 stations)

Author: Sherif Elashmawy
Project: FMI Weather Pipeline
Date: December 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fmi_weather_daily_processing',
    default_args=default_args,
    description='Daily processing of FMI weather data',
    schedule_interval='0 1 * * *',  # Run at 1:00 AM every day
    start_date=days_ago(1),
    catchup=False,  # Don't run for past dates
    tags=['weather', 'fmi', 'bigquery'],
)

# Configuration - YOUR PROJECT!
PROJECT_ID = 'data-analytics-project-482302'
DATASET_ID = 'fmi_weather'

# Selected stations for long-term tracking
# NOTE: Names use ASCII only (no ä, ö, å) to avoid BigQuery job ID errors
SELECTED_STATIONS = [
    {'fmisid': 100971, 'name': 'Turku'},
    {'fmisid': 101846, 'name': 'Rovaniemi_lentoasema'},
]


def get_processing_date(**context):
    """
    Get the date to process (yesterday's data).
    Stores in XCom for other tasks to use.
    """
    # Use current time to calculate yesterday (better for testing)
    from datetime import datetime
    processing_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_suffix = processing_date.replace('-', '')
    
    # Store in XCom
    context['task_instance'].xcom_push(key='processing_date', value=processing_date)
    context['task_instance'].xcom_push(key='date_suffix', value=date_suffix)
    
    logger.info(f"Processing date: {processing_date} (suffix: {date_suffix})")
    return processing_date


# Task 1: Get processing date
get_date_task = PythonOperator(
    task_id='get_processing_date',
    python_callable=get_processing_date,
    provide_context=True,
    dag=dag,
)


# Task 2: Process daily observations
process_daily_observations = BigQueryInsertJobOperator(
    task_id='process_daily_observations',
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.daily_observations_{{{{ ti.xcom_pull(key='date_suffix') }}}}` AS
                
                WITH deduplicated AS (
                  SELECT DISTINCT
                    timestamp,
                    fmisid,
                    location,
                    latitude,
                    longitude,
                    temperature,
                    humidity,
                    wind_speed,
                    wind_direction,
                    pressure,
                    precipitation,
                    cloud_cover,
                    visibility,
                    ingestion_time
                  FROM `{PROJECT_ID}.{DATASET_ID}.raw_observations`
                  WHERE DATE(timestamp) = '{{{{ ti.xcom_pull(key='processing_date') }}}}'
                ),
                
                latest_per_hour AS (
                  SELECT * EXCEPT(row_num)
                  FROM (
                    SELECT *,
                      ROW_NUMBER() OVER (
                        PARTITION BY fmisid, DATE_TRUNC(timestamp, HOUR) 
                        ORDER BY timestamp DESC
                      ) as row_num
                    FROM deduplicated
                  )
                  WHERE row_num = 1
                ),
                
                quality_checked AS (
                  SELECT
                    *,
                    CASE 
                      WHEN temperature IS NULL THEN 'MISSING'
                      WHEN temperature < -50 OR temperature > 50 THEN 'OUTLIER'
                      ELSE 'VALID'
                    END as temperature_quality,
                    CASE 
                      WHEN humidity IS NULL THEN 'MISSING'
                      WHEN humidity < 0 OR humidity > 100 THEN 'OUTLIER'
                      ELSE 'VALID'
                    END as humidity_quality,
                    CASE 
                      WHEN wind_speed IS NULL THEN 'MISSING'
                      WHEN wind_speed < 0 OR wind_speed > 50 THEN 'OUTLIER'
                      ELSE 'VALID'
                    END as wind_speed_quality,
                    CASE 
                      WHEN pressure IS NULL THEN 'MISSING'
                      WHEN pressure < 900 OR pressure > 1100 THEN 'OUTLIER'
                      ELSE 'VALID'
                    END as pressure_quality,
                    (
                      CAST(temperature IS NOT NULL AS INT64) +
                      CAST(humidity IS NOT NULL AS INT64) +
                      CAST(wind_speed IS NOT NULL AS INT64) +
                      CAST(pressure IS NOT NULL AS INT64)
                    ) / 4.0 as data_completeness_score
                  FROM latest_per_hour
                )
                
                SELECT
                  *,
                  CURRENT_TIMESTAMP() as processing_time
                FROM quality_checked
                ORDER BY fmisid, timestamp
            """,
            "useLegacySql": False,
        }
    },
    location='US',
    dag=dag,
)


# Task 3: Create daily summary
create_daily_summary = BigQueryInsertJobOperator(
    task_id='create_daily_summary',
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.daily_summary_{{{{ ti.xcom_pull(key='date_suffix') }}}}` AS
                SELECT
                  fmisid,
                  location,
                  latitude,
                  longitude,
                  DATE(timestamp) as observation_date,
                  COUNT(CASE WHEN temperature IS NOT NULL THEN 1 END) as temperature_count,
                  ROUND(AVG(temperature), 2) as avg_temperature,
                  ROUND(MIN(temperature), 2) as min_temperature,
                  ROUND(MAX(temperature), 2) as max_temperature,
                  ROUND(STDDEV(temperature), 2) as stddev_temperature,
                  COUNT(CASE WHEN humidity IS NOT NULL THEN 1 END) as humidity_count,
                  ROUND(AVG(humidity), 2) as avg_humidity,
                  ROUND(MIN(humidity), 2) as min_humidity,
                  ROUND(MAX(humidity), 2) as max_humidity,
                  COUNT(CASE WHEN wind_speed IS NOT NULL THEN 1 END) as wind_count,
                  ROUND(AVG(wind_speed), 2) as avg_wind_speed,
                  ROUND(MAX(wind_speed), 2) as max_wind_speed,
                  ROUND(AVG(pressure), 2) as avg_pressure,
                  ROUND(MIN(pressure), 2) as min_pressure,
                  ROUND(MAX(pressure), 2) as max_pressure,
                  ROUND(SUM(COALESCE(precipitation, 0)), 2) as total_precipitation,
                  COUNT(*) as total_observations,
                  ROUND(AVG(data_completeness_score), 3) as avg_completeness_score,
                  COUNTIF(temperature_quality = 'OUTLIER') as temperature_outliers,
                  COUNTIF(humidity_quality = 'OUTLIER') as humidity_outliers,
                  CURRENT_TIMESTAMP() as processing_time
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_observations_{{{{ ti.xcom_pull(key='date_suffix') }}}}`
                GROUP BY fmisid, location, latitude, longitude, observation_date
                ORDER BY fmisid
            """,
            "useLegacySql": False,
        }
    },
    location='US',
    dag=dag,
)


# Task 4: Generate quality report
generate_quality_report = BigQueryInsertJobOperator(
    task_id='generate_quality_report',
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.quality_report_{{{{ ti.xcom_pull(key='date_suffix') }}}}` AS
                WITH quality_metrics AS (
                  SELECT
                    DATE(timestamp) as report_date,
                    COUNT(DISTINCT fmisid) as active_stations,
                    COUNT(*) as total_observations,
                    COUNTIF(temperature IS NULL) as missing_temperature,
                    COUNTIF(humidity IS NULL) as missing_humidity,
                    COUNTIF(wind_speed IS NULL) as missing_wind_speed,
                    COUNTIF(pressure IS NULL) as missing_pressure,
                    COUNTIF(temperature_quality = 'OUTLIER') as outlier_temperature,
                    COUNTIF(humidity_quality = 'OUTLIER') as outlier_humidity,
                    COUNTIF(wind_speed_quality = 'OUTLIER') as outlier_wind_speed,
                    COUNTIF(pressure_quality = 'OUTLIER') as outlier_pressure,
                    ROUND(COUNTIF(temperature_quality = 'VALID') * 100.0 / COUNT(*), 2) as temperature_valid_pct,
                    ROUND(COUNTIF(humidity_quality = 'VALID') * 100.0 / COUNT(*), 2) as humidity_valid_pct,
                    ROUND(COUNTIF(wind_speed_quality = 'VALID') * 100.0 / COUNT(*), 2) as wind_speed_valid_pct,
                    ROUND(COUNTIF(pressure_quality = 'VALID') * 100.0 / COUNT(*), 2) as pressure_valid_pct,
                    ROUND(AVG(data_completeness_score), 3) as avg_completeness_score
                  FROM `{PROJECT_ID}.{DATASET_ID}.daily_observations_{{{{ ti.xcom_pull(key='date_suffix') }}}}`
                  GROUP BY report_date
                )
                SELECT 
                  *,
                  CASE 
                    WHEN avg_completeness_score >= 0.9 THEN 'EXCELLENT'
                    WHEN avg_completeness_score >= 0.7 THEN 'GOOD'
                    WHEN avg_completeness_score >= 0.5 THEN 'FAIR'
                    ELSE 'POOR'
                  END as overall_quality_rating,
                  CURRENT_TIMESTAMP() as generated_at
                FROM quality_metrics
            """,
            "useLegacySql": False,
        }
    },
    location='US',
    dag=dag,
)


# Task 5-9: Update long-term station tables (FREE TIER COMPATIBLE!)
def create_update_longterm_task(station_info):
    """
    Create a task to update long-term table for a specific station.
    Uses CREATE OR REPLACE to avoid DML restrictions in free tier.
    """
    fmisid = station_info['fmisid']
    name = station_info['name']
    
    return BigQueryInsertJobOperator(
        task_id=f'update_longterm_{name}',
        configuration={
            "query": {
                "query": f"""
                    -- Create or replace table with new data appended to existing data
                    -- This uses DDL (CREATE OR REPLACE) instead of DML (INSERT)
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.station_{fmisid}_longterm`
                    PARTITION BY DATE(timestamp)
                    AS
                    
                    -- Get existing data (if table exists)
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.station_{fmisid}_longterm`
                    WHERE TRUE  -- This will fail silently if table doesn't exist
                    
                    UNION DISTINCT
                    
                    -- Add new data from today's processing
                    SELECT 
                      timestamp,
                      temperature,
                      humidity,
                      wind_speed,
                      wind_direction,
                      pressure,
                      precipitation,
                      cloud_cover,
                      visibility,
                      data_completeness_score,
                      processing_time
                    FROM `{PROJECT_ID}.{DATASET_ID}.daily_observations_{{{{ ti.xcom_pull(key='date_suffix') }}}}`
                    WHERE fmisid = {fmisid}
                """,
                "useLegacySql": False,
            }
        },
        location='US',
        dag=dag,
    )


# Create update tasks for all selected stations
update_longterm_tasks = [
    create_update_longterm_task(station) 
    for station in SELECTED_STATIONS
]


# Define task dependencies (execution order)
get_date_task >> process_daily_observations
process_daily_observations >> [create_daily_summary, generate_quality_report]

# Both summary and quality report must complete before longterm updates
for longterm_task in update_longterm_tasks:
    [create_daily_summary, generate_quality_report] >> longterm_task