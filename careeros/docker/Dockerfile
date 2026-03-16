FROM python:3.11-slim

# cache-bust: 1773000001
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org && \
    pip install --no-cache-dir \
        --trusted-host pypi.org \
        --trusted-host files.pythonhosted.org \
        -r requirements.txt

COPY app/ ./app/
COPY alembic.ini .

RUN useradd --create-home --shell /bin/bash --uid 1001 careeros \
    && chown -R careeros:careeros /app
USER careeros

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
