FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY dashboard /app/dashboard

CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8501"]
