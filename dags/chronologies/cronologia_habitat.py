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
import sqlparse
import os 

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


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

def crear_script_cron_hab():

    nombre_archivo_sql = "cronologia_habitat.sql"


    hoy  = datetime.datetime.now()
    if hoy.day <= 15:
        fecha_inicial = f'{hoy.year}{hoy.month}01'
    else:
        fecha_inicial = f'{hoy.year}{hoy.month}16'

    today = datetime.date.today()
    first = today.replace(day=1)

    periodo_inicial = (first - relativedelta(months=+5)).strftime("%Y%m")
    periodo_final = (first - relativedelta(months=+2)).strftime("%Y%m") 

    fecha_final = hoy.strftime("%Y%m%d")
    query1 = f"""
        DECLARE @rut_cliente int
        declare @fecha_ini int
        declare @fecha_fin int
        declare @periodo_ini int
        declare @periodo_fin int

        set @fecha_ini={fecha_inicial}
        set @fecha_fin={fecha_final}
        set @periodo_ini={periodo_inicial}
        set @periodo_fin={periodo_final}

        EXECUTE [up_cob_cronoprejudicial_fechas_b_s_auto]  98000100, @fecha_ini, @fecha_fin, @periodo_ini, @periodo_fin
    """

    hook = MsSqlHook(mssql_conn_id=database)
    try:
        hook.run("use webcob; DROP TABLE #tmp_liquidacion")
    except Exception as e:
        logging.error(e)
    
    try:
        hook.run(query1, autocommit=True)
    except Exception as e:
        logging.error(e)

    try:
        df = hook.get_pandas_df("SELECT * FROM CRON_HAB_TXT_AUTO")
    except Exception as e:
        logging.error(e)

    try:
        df = hook.get_pandas_df("SELECT * FROM CRON_HAB_XLSX_AUTO")
        file_path = os.path.join("output", "chronologies", f"cronologia_hab_{fecha_final}.xlsx")
        df.to_excel(file_path)
    except Exception as e:
        logging.error(e)
    try:
        df = hook.run("DROP TABLE CRON_HAB_TXT_AUTO", autocommit=True)
        file_path = os.path.join("output", "chronologies", f"cronologia_hab_{fecha_final}.txt")
        df.tO_csv(file_path, sep=" ")
    except Exception as e:
        logging.error(e)

    try:
        df = hook.run("DROP TABLE CRON_HAB_XLSX_AUTO", autocommit=True)
    except Exception as e:
        logging.error(e)



with DAG(
    "crear_cronologia_habitat",
    default_args=default_args,
    description="crea los archivos de Cronologia Habitat",
    schedule_interval=None,
    max_active_runs=1,
    concurrency=4,
    tags=["cronologias", "sql server"],
) as dag:
    
    crear_sql_script_hab = PythonOperator(
        task_id="crear_script_cron_hab", 
        python_callable=crear_script_cron_hab
    )
   
   
