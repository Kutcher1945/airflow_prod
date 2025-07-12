FROM apache/airflow:2.8.1

USER root

# Optional: Install OS packages (if you need any additional system dependencies like libpq-dev for PostgreSQL)
# Uncomment the next line if needed:
# RUN apt-get update && apt-get install -y libpq-dev gcc curl git && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to avoid running pip as root
USER airflow

# Copy the requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Optionally, you can also add .env file copying (but it's not recommended to hardcode secrets in Dockerfile)
# If you really want to use .env in your container, you can uncomment the line below:
# COPY .env /opt/airflow/.env

# Run Airflow as default entrypoint (this is already the default in the base image)
