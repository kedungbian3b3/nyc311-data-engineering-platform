from __future__ import annotations

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


def trigger_ingestion(**context):
    """Call the ingestion API. Limit can be overridden in Airflow Trigger DAG config."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    default_limit = int(Variable.get("NYC311_DEFAULT_LIMIT", default_var="5000"))
    limit = int(conf.get("limit", default_limit))
    days_back = conf.get("days_back")

    base_url = Variable.get("INGESTION_API_URL", default_var=os.getenv("INGESTION_API_URL", "http://ingestion-api:8000"))
    params = {"limit": limit, "allow_fixture_fallback": "true"}
    if days_back:
        params["days_back"] = int(days_back)

    response = requests.post(f"{base_url}/ingest", params=params, timeout=600)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))


with DAG(
    dag_id="nyc_311_end_to_end_pipeline",
    description="Public API ingestion -> PostgreSQL raw -> dbt staging/marts -> dashboard-ready tables",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["data-engineering", "nyc-311", "public-api", "dbt", "dashboard"],
) as dag:
    ingest_public_api_to_raw = PythonOperator(
        task_id="ingest_public_api_to_raw_postgres",
        python_callable=trigger_ingestion,
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug_connection",
        bash_command="cd /opt/airflow/dbt/projects/nyc_311_analytics && dbt debug --profiles-dir /opt/airflow/dbt/.dbt",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_transformations",
        bash_command="cd /opt/airflow/dbt/projects/nyc_311_analytics && dbt run --profiles-dir /opt/airflow/dbt/.dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command="cd /opt/airflow/dbt/projects/nyc_311_analytics && dbt test --profiles-dir /opt/airflow/dbt/.dbt",
    )

    ingest_public_api_to_raw >> dbt_debug >> dbt_run >> dbt_test
