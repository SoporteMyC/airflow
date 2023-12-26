import warnings
import logging
from airflow.models import DAG
from airflow.utils.dates import timedelta, days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

try:
    database = Variable.get("current_database")
except:
    logging.error("Error al conseguir la variable de base de datos actual.")

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

def check_email():
    hook = MsSqlHook(mssql_conn_id=database) 
    
    try:
        df = hook.get_pandas_df("select * from webcob.dbo.ut_cob_parametros_m where cod_app='SMAIL' and cod_parametro='SMTPL' ;")
        print(f"Correo actual: {df['val_texto']}")
    except Exception as e:
        logging.error(e)


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

    drop_temporary_tables = PythonOperator(
        task_id="revisar_correo",
        python_callable=check_email
    )