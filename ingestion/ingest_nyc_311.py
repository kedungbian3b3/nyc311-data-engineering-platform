"""Ingest NYC 311 public API data into PostgreSQL raw schema.

The script is designed to be used by:
1. Airflow DAG: scheduled or manual orchestration.
2. n8n workflow: manual data crawl demo.
3. ingestion-api service: HTTP endpoint for lightweight triggering.

Default source is the public Socrata endpoint, no account required.
A local fixture is included only for dry-run tests and demo fallback when the API is unreachable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

SOURCE_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "location_type",
    "incident_zip",
    "city",
    "borough",
    "status",
    "resolution_description",
    "latitude",
    "longitude",
]

RAW_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.nyc_311_service_requests (
    unique_key TEXT PRIMARY KEY,
    created_date TIMESTAMPTZ,
    closed_date TIMESTAMPTZ,
    agency TEXT,
    agency_name TEXT,
    complaint_type TEXT,
    descriptor TEXT,
    location_type TEXT,
    incident_zip TEXT,
    city TEXT,
    borough TEXT,
    status TEXT,
    resolution_description TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    ingest_batch_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_311_created_date ON raw.nyc_311_service_requests (created_date);
CREATE INDEX IF NOT EXISTS idx_raw_311_borough ON raw.nyc_311_service_requests (borough);
CREATE INDEX IF NOT EXISTS idx_raw_311_complaint_type ON raw.nyc_311_service_requests (complaint_type);
CREATE INDEX IF NOT EXISTS idx_raw_311_agency ON raw.nyc_311_service_requests (agency);
"""

UPSERT_SQL = """
INSERT INTO raw.nyc_311_service_requests (
    unique_key, created_date, closed_date, agency, agency_name, complaint_type,
    descriptor, location_type, incident_zip, city, borough, status,
    resolution_description, latitude, longitude, ingest_batch_id, ingested_at
)
VALUES %s
ON CONFLICT (unique_key) DO UPDATE SET
    created_date = EXCLUDED.created_date,
    closed_date = EXCLUDED.closed_date,
    agency = EXCLUDED.agency,
    agency_name = EXCLUDED.agency_name,
    complaint_type = EXCLUDED.complaint_type,
    descriptor = EXCLUDED.descriptor,
    location_type = EXCLUDED.location_type,
    incident_zip = EXCLUDED.incident_zip,
    city = EXCLUDED.city,
    borough = EXCLUDED.borough,
    status = EXCLUDED.status,
    resolution_description = EXCLUDED.resolution_description,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    ingest_batch_id = EXCLUDED.ingest_batch_id,
    ingested_at = EXCLUDED.ingested_at;
"""


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        normalized = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def parse_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def normalize_record(record: Dict[str, Any], batch_id: str, ingested_at: datetime) -> Optional[tuple]:
    unique_key = clean_text(record.get("unique_key"))
    if not unique_key:
        return None

    return (
        unique_key,
        parse_datetime(record.get("created_date")),
        parse_datetime(record.get("closed_date")),
        clean_text(record.get("agency")),
        clean_text(record.get("agency_name")),
        clean_text(record.get("complaint_type")),
        clean_text(record.get("descriptor")),
        clean_text(record.get("location_type")),
        clean_text(record.get("incident_zip")),
        clean_text(record.get("city")),
        clean_text(record.get("borough")),
        clean_text(record.get("status")),
        clean_text(record.get("resolution_description")),
        parse_float(record.get("latitude")),
        parse_float(record.get("longitude")),
        batch_id,
        ingested_at,
    )


def load_fixture() -> List[Dict[str, Any]]:
    fixture_path = Path(__file__).with_name("sample_nyc_311.json")
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def fetch_from_api(limit: int, days_back: Optional[int] = None) -> List[Dict[str, Any]]:
    import requests

    api_url = os.getenv("NYC311_API_URL", "https://data.cityofnewyork.us/resource/erm2-nwe9.json")
    params: Dict[str, Any] = {
        "$select": ",".join(SOURCE_COLUMNS),
        "$limit": int(limit),
        "$order": "created_date DESC",
    }

    if days_back is not None and days_back > 0:
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S")
        params["$where"] = f"created_date >= '{since}'"

    app_token = os.getenv("SOCRATA_APP_TOKEN")
    headers = {"X-App-Token": app_token} if app_token else {}

    response = requests.get(api_url, params=params, headers=headers, timeout=90)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected API response shape: {type(payload).__name__}")
    return payload


def get_connection():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("ANALYTICS_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_DB_PORT", "5432")),
        dbname=os.getenv("ANALYTICS_DB_NAME", "analytics"),
        user=os.getenv("ANALYTICS_DB_USER", "airflow"),
        password=os.getenv("ANALYTICS_DB_PASSWORD", "airflow"),
        connect_timeout=20,
    )


def ensure_raw_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(RAW_TABLE_DDL)
    conn.commit()


def upsert_rows(conn, rows: Iterable[tuple]) -> int:
    from psycopg2.extras import execute_values

    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


def run_ingestion(
    *,
    limit: Optional[int] = None,
    days_back: Optional[int] = None,
    source: str = "api",
    dry_run: bool = False,
    allow_fixture_fallback: Optional[bool] = None,
) -> Dict[str, Any]:
    effective_limit = int(limit or os.getenv("INGEST_DEFAULT_LIMIT", "5000"))
    fallback_enabled = (
        str(os.getenv("INGEST_ALLOW_FIXTURE_FALLBACK", "false")).lower() in {"1", "true", "yes"}
        if allow_fixture_fallback is None
        else allow_fixture_fallback
    )

    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)

    try:
        records = load_fixture() if source == "fixture" else fetch_from_api(effective_limit, days_back)
        actual_source = source
    except Exception as exc:
        if not fallback_enabled:
            raise
        records = load_fixture()
        actual_source = "fixture_fallback"
        print(f"WARNING: API ingestion failed, loaded fixture fallback instead: {exc}", file=sys.stderr)

    rows = [row for row in (normalize_record(r, batch_id, ingested_at) for r in records) if row]

    summary: Dict[str, Any] = {
        "batch_id": batch_id,
        "source": actual_source,
        "requested_limit": effective_limit,
        "fetched_records": len(records),
        "valid_records": len(rows),
        "dry_run": dry_run,
        "ingested_at": ingested_at.isoformat(),
    }

    if dry_run:
        summary["sample_unique_keys"] = [row[0] for row in rows[:5]]
        return summary

    conn = get_connection()
    try:
        ensure_raw_table(conn)
        summary["upserted_records"] = upsert_rows(conn, rows)
    finally:
        conn.close()
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest NYC 311 public data into PostgreSQL")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of records to fetch from API")
    parser.add_argument("--days-back", type=int, default=None, help="Only fetch requests created in the last N days")
    parser.add_argument("--source", choices=["api", "fixture"], default="api", help="Data source for ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Validate parsing without writing to PostgreSQL")
    parser.add_argument("--allow-fixture-fallback", action="store_true", help="Use fixture data when API is unavailable")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = run_ingestion(
        limit=args.limit,
        days_back=args.days_back,
        source=args.source,
        dry_run=args.dry_run,
        allow_fixture_fallback=args.allow_fixture_fallback or None,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
