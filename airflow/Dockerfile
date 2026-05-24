FROM astrocrpublic.azurecr.io/runtime:3.2-4

FROM quay.io/astronomer/astro-runtime:10.5.0

# Cài dbt-bigquery
RUN pip install --no-cache-dir dbt-bigquery==1.7.0

# Copy dbt project vào image
COPY --chown=astro:astro dbt_project /usr/local/airflow/dbt_project