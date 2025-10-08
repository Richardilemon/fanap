FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY airflow/scripts/ ./scripts/
COPY utils/ ./utils/
# COPY airflow/scripts/db_config.py .
CMD ["python", "airflow/scripts/load_fixtures.py"]