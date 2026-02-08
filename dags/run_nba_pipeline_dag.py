from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

default_args = {
    'owner': 'nathan',
}

dag = DAG(
    'run_daily_nba_data_pipeline',
    default_args=default_args,
    description='Run NBA data pipeline daily to feed v2 app',
    schedule_interval='0 13 * * *',
    start_date=datetime(2024, 2, 3),
    catchup=False
)


bash_cmd = f'/usr/bin/Rscript {AIRFLOW_HOME}/r_scripts/nba_data_scraper.R'

run_nba_data_scraper_r_script = BashOperator(
    task_id=f"run_nba_data_scraper_r_script",
    bash_command=bash_cmd,
    dag=dag,
)

run_nba_data_scraper_r_script 
