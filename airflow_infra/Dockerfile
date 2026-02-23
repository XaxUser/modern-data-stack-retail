FROM apache/airflow:2.9.3-python3.11

USER airflow

# 1. On installe le plugin Airflow (Cosmos) dans le salon principal
RUN pip install --no-cache-dir astronomer-cosmos

# 2. On crée un venv isolée UNIQUEMENT pour dbt
# Ainsi, Airflow et dbt ne croiseront jamais leurs dépendances !
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-snowflake