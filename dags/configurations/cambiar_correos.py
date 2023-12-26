import warnings
import logging
from airflow.models import DAG
from airflow.utils.dates import timedelta, days_ago
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


default_args = {
    "owner": "adrian",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["soporte@menaresycia.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "catchup_by_default": False,
    "execution_timeout": timedelta(seconds=1800),
}


with DAG(
    "cambiar_correos_webcob",
    default_args=default_args,
    description="cambia el correo utilizado en webcob.",
    schedule_interval=None,
    max_active_runs=1,
    concurrency=4,
    tags=["informes", "sql server"],
) as dag:
    
    create_hab_table = MsSqlOperator(
        task_id="ejecutar_script_cambio_correos",
        mssql_conn_id="mssql_menares_34",
        sql="queries/cambia_correos_webcob.sql",
        split_statements=False,
        autocommit=True,
        dag=dag
    )