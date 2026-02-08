FROM apache/airflow:2.8.1-python3.10

USER root

ENV VENV_PATH="/usr/local/airflow/dbt_venv"
ENV PIP_ROOT_USER_ACTION=ignore
ENV PIP_USER=false


# Install Airflow runtime deps into system Python
RUN python -m pip install --no-cache-dir asyncpg faker minio spotipy

# Create isolated venv for dbt + cosmos
RUN python -m venv $VENV_PATH && \
    $VENV_PATH/bin/pip install --upgrade pip && \
    $VENV_PATH/bin/pip install --no-cache-dir \
        astronomer-cosmos \
        dbt-postgres

# Optional: expose dbt command without activating venv
RUN ln -s $VENV_PATH/bin/dbt /usr/local/bin/dbt

USER airflow