# Start with a base Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install dependencies
COPY scripts/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install numpy==1.21.6 pandas==1.3.5 sqlalchemy

# Copy the scripts into the container
COPY scripts /app/scripts

# Set the default command to run the load_players.py script
CMD ["python", "scripts/load.py"]
