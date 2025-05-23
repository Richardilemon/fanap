FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts/ ./scripts/
COPY utils/ ./utils/
COPY db_config.py .
CMD ["python", "scripts/load_fixtures.py"]