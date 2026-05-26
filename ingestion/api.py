from __future__ import annotations

from fastapi import FastAPI, Query
from pydantic import BaseModel

from ingestion.ingest_nyc_311 import run_ingestion

app = FastAPI(title="NYC 311 Ingestion API", version="1.0.0")


class HealthResponse(BaseModel):
    status: str


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/ingest")
def ingest(
    limit: int | None = Query(default=None, ge=1, le=50000),
    days_back: int | None = Query(default=None, ge=1, le=3650),
    allow_fixture_fallback: bool = Query(default=True),
):
    """Trigger an ingestion batch and upsert raw rows into PostgreSQL."""
    return run_ingestion(
        limit=limit,
        days_back=days_back,
        source="api",
        dry_run=False,
        allow_fixture_fallback=allow_fixture_fallback,
    )
