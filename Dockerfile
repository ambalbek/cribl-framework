FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && find /usr/local/lib -type d -name "tests"       -exec rm -rf {} + 2>/dev/null; true \
    && find /usr/local/lib -type d -name "test"        -exec rm -rf {} + 2>/dev/null; true \
    && find /usr/local/lib -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true

COPY *.py ./
COPY *.json ./
COPY templates/ templates/

# config.json is NOT baked in — mount at runtime:
#   -v ./config.json:/app/config.json:ro

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')" || exit 1

CMD ["python", "app.py"]
