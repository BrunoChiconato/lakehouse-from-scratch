# ---- Base Stage ----
FROM python:3.13-slim-bullseye AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64


# ---- Builder Stage ----
FROM base AS builder
WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


# ---- Runtime Stage ----
FROM base AS runtime
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

COPY src ./src
COPY .env .

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
RUN chown -R appuser:appgroup /app

ENV PATH="/opt/venv/bin:$PATH"
USER appuser