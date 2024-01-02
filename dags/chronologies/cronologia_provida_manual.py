import warnings
import logging
from airflow.models import DAG
from airflow.utils.dates import timedelta, days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models import Variable
import datetime
from dateutil.relativedelta import relativedelta
import os 
import time

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

hoy  = datetime.datetime.now()
if hoy.day <= 15:
    fecha_inicial = f'{hoy.year}{hoy.month}01'
else:
    fecha_inicial = f'{hoy.year}{hoy.month}16'

today = datetime.date.today()
first = today.replace(day=1)
day_before = today - timedelta(days=1)

fecha = f"{day_before.year}-{day_before.month}-{day_before.day}"
periodo = f"{day_before.year}{day_before.month}"

query1 = f"""
    USE WEBCOB

    declare @fecha date

    set @fecha= '{fecha}'

    exec CRONOLOGIA_{periodo}_CAR @fecha

    """
query2 = f"""
    USE WEBCOB

    declare @fecha date

    set @fecha= '{fecha}'

    exec CRONOLOGIA_{periodo}_DNP @fecha

    """

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


with DAG(
    "crear_cronologia_provida_manual",
    default_args=default_args,
    description="Vuelve a generar la cronologia de Provida",
    schedule_interval=None,
    max_active_runs=1,
    concurrency=4,
    tags=["cronologias", "sql server", "provida"],
) as dag:
    
    ejecutar_query1 = MsSqlOperator(
        task_id="ejecutar_cronologia_CAR",
        mssql_conn_id=database,
        sql=query1,
        autocommit=True
    )

    ejecutar_query2 = MsSqlOperator(
        task_id="ejecutar_cronologia_DNP",
        mssql_conn_id=database,
        sql=query2,
        autocommit=True
    )

    ejecutar_query1 >> ejecutar_query2
