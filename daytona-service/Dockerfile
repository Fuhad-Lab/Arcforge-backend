FROM python:3.12-slim AS base

WORKDIR /app

# --- Install build deps & project ---
COPY pyproject.toml .
RUN pip install --no-cache-dir . 2>&1 | tail -5

# --- Application code ---
COPY app/ app/

# --- Runtime ---
EXPOSE 8000

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
