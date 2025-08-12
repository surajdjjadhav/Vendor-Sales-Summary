# Dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root

# Install build tools and mysql client dev headers
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    libssl-dev \
    libffi-dev && \
    apt-get clean

WORKDIR /opt/airflow

COPY ./requirements.txt ./requirements.txt
# COPY ./setup.py ./setup.py
COPY ./pyproject.toml ./pyproject.toml
COPY ./src ./src
COPY ./.project-root ./.project-root

COPY ./entrypoint.sh ./entrypoint.sh

RUN chmod +x entrypoint.sh



RUN chown -R airflow: /opt/airflow

USER airflow

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt


