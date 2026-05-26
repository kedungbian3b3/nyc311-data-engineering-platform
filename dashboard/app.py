from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="NYC 311 Analytics Dashboard", version="1.0.0")


def json_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_DB_PORT", "5432")),
        dbname=os.getenv("ANALYTICS_DB_NAME", "analytics"),
        user=os.getenv("ANALYTICS_DB_USER", "airflow"),
        password=os.getenv("ANALYTICS_DB_PASSWORD", "airflow"),
        connect_timeout=10,
    )


def query(sql: str) -> List[Dict[str, Any]]:
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_dashboard_data() -> Dict[str, Any]:
    daily = query("""
        select created_day, total_requests, closed_requests, open_or_other_requests, avg_resolution_hours
        from marts.mart_daily_requests
        order by created_day
    """)
    complaints = query("""
        select complaint_type, total_requests, avg_resolution_hours
        from marts.mart_complaint_types
        order by total_requests desc
        limit 10
    """)
    boroughs = query("""
        select borough, sum(total_requests)::int as total_requests
        from marts.mart_borough_complaints
        group by borough
        order by total_requests desc
    """)
    status = query("""
        select status, total_requests, percentage
        from marts.mart_status_summary
        order by total_requests desc
    """)
    agency = query("""
        select agency, total_requests, avg_resolution_hours, median_resolution_hours
        from marts.mart_agency_performance
        order by total_requests desc
        limit 10
    """)
    resolution = query("""
        select borough, avg_resolution_hours, median_resolution_hours, resolved_records
        from marts.mart_resolution_metrics
        order by avg_resolution_hours desc
    """)

    total_requests = sum(int(row.get("total_requests") or 0) for row in daily)
    closed_requests = sum(int(row.get("closed_requests") or 0) for row in daily)
    avg_resolution_values = [float(row.get("avg_resolution_hours") or 0) for row in daily if row.get("avg_resolution_hours") is not None]
    avg_resolution = round(sum(avg_resolution_values) / len(avg_resolution_values), 2) if avg_resolution_values else 0

    return {
        "daily": daily,
        "complaints": complaints,
        "boroughs": boroughs,
        "status": status,
        "agency": agency,
        "resolution": resolution,
        "kpis": {
            "total_requests": total_requests,
            "closed_requests": closed_requests,
            "open_or_other_requests": max(total_requests - closed_requests, 0),
            "avg_resolution_hours": avg_resolution,
        },
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    data = get_dashboard_data()
    has_data = bool(data["daily"] or data["complaints"])
    data_json = json.dumps(data, default=json_default)

    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NYC 311 Analytics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{ --bg: #0f172a; --card: #111827; --muted: #94a3b8; --text: #e5e7eb; --accent: #38bdf8; }}
    body {{ margin: 0; font-family: Inter, Segoe UI, Arial, sans-serif; background: linear-gradient(135deg, #0f172a, #111827); color: var(--text); }}
    .container {{ max-width: 1280px; margin: 0 auto; padding: 28px; }}
    .hero {{ display: flex; justify-content: space-between; gap: 20px; align-items: end; margin-bottom: 24px; }}
    h1 {{ margin: 0; font-size: 32px; letter-spacing: -0.02em; }}
    .subtitle {{ color: var(--muted); margin-top: 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin-bottom: 16px; }}
    .charts {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }}
    .card {{ background: rgba(17, 24, 39, 0.88); border: 1px solid rgba(148, 163, 184, .18); border-radius: 18px; padding: 18px; box-shadow: 0 20px 40px rgba(0,0,0,.25); }}
    .kpi-label {{ color: var(--muted); font-size: 13px; }}
    .kpi-value {{ font-size: 28px; font-weight: 800; margin-top: 6px; }}
    .chart-title {{ font-weight: 700; margin-bottom: 12px; }}
    canvas {{ width: 100%; max-height: 330px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid rgba(148, 163, 184, .15); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .notice {{ border: 1px dashed var(--accent); color: #bae6fd; padding: 16px; border-radius: 16px; margin-bottom: 16px; }}
    @media (max-width: 980px) {{ .grid, .charts {{ grid-template-columns: 1fr; }} .hero {{ display: block; }} }}
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <div>
        <h1>NYC 311 Service Requests Analytics</h1>
        <div class="subtitle">Public API → PostgreSQL → dbt marts → dashboard-ready insights</div>
      </div>
      <div class="subtitle">Refresh: run Airflow DAG <b>nyc_311_end_to_end_pipeline</b></div>
    </div>

    {'' if has_data else '<div class="notice">No mart data found yet. Open Airflow at http://localhost:8080 and run the DAG, or run the n8n ingestion workflow then dbt.</div>'}

    <div class="grid">
      <div class="card"><div class="kpi-label">Total Requests</div><div class="kpi-value" id="kpi-total">0</div></div>
      <div class="card"><div class="kpi-label">Closed Requests</div><div class="kpi-value" id="kpi-closed">0</div></div>
      <div class="card"><div class="kpi-label">Open / Other</div><div class="kpi-value" id="kpi-open">0</div></div>
      <div class="card"><div class="kpi-label">Avg Resolution Hours</div><div class="kpi-value" id="kpi-resolution">0</div></div>
    </div>

    <div class="charts">
      <div class="card"><div class="chart-title">Daily Request Volume</div><canvas id="dailyChart"></canvas></div>
      <div class="card"><div class="chart-title">Top Complaint Types</div><canvas id="complaintChart"></canvas></div>
      <div class="card"><div class="chart-title">Requests by Borough</div><canvas id="boroughChart"></canvas></div>
      <div class="card"><div class="chart-title">Status Distribution</div><canvas id="statusChart"></canvas></div>
      <div class="card"><div class="chart-title">Agency Workload</div><canvas id="agencyChart"></canvas></div>
      <div class="card"><div class="chart-title">Resolution Hours by Borough</div><canvas id="resolutionChart"></canvas></div>
    </div>

    <div class="card" style="margin-top:16px;">
      <div class="chart-title">Top Agencies Table</div>
      <table id="agencyTable"><thead><tr><th>Agency</th><th>Total Requests</th><th>Avg Resolution Hours</th><th>Median Resolution Hours</th></tr></thead><tbody></tbody></table>
    </div>
  </div>

<script>
const data = {data_json};
const fmt = new Intl.NumberFormat('en-US');
document.getElementById('kpi-total').innerText = fmt.format(data.kpis.total_requests || 0);
document.getElementById('kpi-closed').innerText = fmt.format(data.kpis.closed_requests || 0);
document.getElementById('kpi-open').innerText = fmt.format(data.kpis.open_or_other_requests || 0);
document.getElementById('kpi-resolution').innerText = data.kpis.avg_resolution_hours || 0;

function chart(id, type, labels, values, label) {{
  return new Chart(document.getElementById(id), {{
    type,
    data: {{ labels, datasets: [{{ label, data: values, borderWidth: 2 }}] }},
    options: {{ responsive: true, plugins: {{ legend: {{ display: type !== 'bar' }} }}, scales: type === 'pie' || type === 'doughnut' ? {{}} : {{ y: {{ beginAtZero: true }} }} }}
  }});
}}

chart('dailyChart', 'line', data.daily.map(x => x.created_day), data.daily.map(x => x.total_requests), 'Requests');
chart('complaintChart', 'bar', data.complaints.map(x => x.complaint_type), data.complaints.map(x => x.total_requests), 'Requests');
chart('boroughChart', 'doughnut', data.boroughs.map(x => x.borough), data.boroughs.map(x => x.total_requests), 'Requests');
chart('statusChart', 'pie', data.status.map(x => x.status), data.status.map(x => x.total_requests), 'Requests');
chart('agencyChart', 'bar', data.agency.map(x => x.agency), data.agency.map(x => x.total_requests), 'Requests');
chart('resolutionChart', 'bar', data.resolution.map(x => x.borough), data.resolution.map(x => x.avg_resolution_hours), 'Avg Hours');

const tbody = document.querySelector('#agencyTable tbody');
for (const row of data.agency) {{
  const tr = document.createElement('tr');
  tr.innerHTML = `<td>${{row.agency || ''}}</td><td>${{fmt.format(row.total_requests || 0)}}</td><td>${{row.avg_resolution_hours ?? ''}}</td><td>${{row.median_resolution_hours ?? ''}}</td>`;
  tbody.appendChild(tr);
}}
</script>
</body>
</html>
"""
