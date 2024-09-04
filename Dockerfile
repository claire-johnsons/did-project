FROM apache/airflow:2.10.0

USER root

# Install Essential
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        vim git telnet\
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow

# TODO: consider install requirements here
RUN python -m pip install --upgrade pip
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
