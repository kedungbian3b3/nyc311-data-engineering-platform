# Quickstart

```bash
# 1. Open this folder in Visual Studio Code
cd nyc311-data-engineering-platform

# 2. Build and start every service with one compose file
docker compose up --build -d

# 3. Check services
docker compose ps
```

Open:

| Tool | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| n8n | http://localhost:5678 | no login in local mode |
| Dashboard | http://localhost:8501 | no login |
| Ingestion API health | http://localhost:8000/health | no login |

Run pipeline:

1. Open Airflow.
2. Open DAG `nyc_311_end_to_end_pipeline`.
3. Click **Trigger DAG**.
4. Wait for these tasks to pass:
   - `ingest_public_api_to_raw_postgres`
   - `dbt_debug_connection`
   - `dbt_run_transformations`
   - `dbt_test_data_quality`
5. Open the dashboard again: http://localhost:8501

Optional n8n ingestion demo:

1. Open n8n: http://localhost:5678
2. Open workflow `NYC 311 - Ingest public API to PostgreSQL`.
3. Click **Execute Workflow**.
4. Then run the dbt tasks from Airflow, or trigger the full Airflow DAG.

Useful commands:

```bash
# Logs
 docker compose logs -f airflow-scheduler
 docker compose logs -f ingestion-api
 docker compose logs -f dashboard

# Reset all generated containers and volumes
 docker compose down -v

# Run dbt manually inside Airflow container
 docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt/projects/nyc_311_analytics && dbt run --profiles-dir /opt/airflow/dbt/.dbt"
```
