FROM apache/airflow:2.8.1

USER root

# Optional: install OS packages
# RUN apt-get update && apt-get install -y \
#     libpq-dev gcc curl git && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to airflow user before pip install
USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
