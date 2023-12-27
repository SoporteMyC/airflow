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

periodo_inicial = (first - relativedelta(months=+5)).strftime("%Y%m")
periodo_final = (first - relativedelta(months=+2)).strftime("%Y%m") 

fecha_final = hoy.strftime("%Y%m%d")

query1 = f"""
        USE WEBCOB

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

'''def ejecutar_procedimiento():

    query1 = f"""
        USE WEBCOB

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
        logging.error(e)'''


def obtener_datos():

    
    hook = MsSqlHook(mssql_conn_id=database)
    contador = 0
    listo1 = False
    listo2 = False
    while(contador < 13):
        time.sleep(300)
        try:
            df = hook.get_pandas_df("SELECT * FROM WEBCOB.dbo.CRON_HAB_TXT_AUTO")
            file_path = os.path.join("output", "chronologies", f"cronologia_hab_{fecha_final}.txt")
            df.to_csv(file_path, sep=" ")
            listo1 = True
        except Exception as e:
            logging.error(e)

        try:
            df = hook.get_pandas_df("SELECT * FROM WEBCOB.dbo.CRON_HAB_XLSX_AUTO")
            file_path = os.path.join("output", "chronologies", f"cronologia_hab_{fecha_final}.xlsx")
            df.to_excel(file_path)
            listo2 = True
        except Exception as e:
            logging.error(e)
        contador += 1
        if listo1 is True and listo2 is True:
            return

def drop_tables():

    hook = MsSqlHook(mssql_conn_id=database)

    try:
        df = hook.run("DROP TABLE WEBCOB.dbo.CRON_HAB_TXT_AUTO", autocommit=True)
    except Exception as e:
        logging.error(e)

    try:
        df = hook.run("DROP TABLE WEBCOB.dbo.CRON_HAB_XLSX_AUTO", autocommit=True)
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
    
    borrar_temp = MsSqlOperator(
        task_id="borrar_temp",
        mssql_conn_id=database,
        sql="use webcob; DROP TABLE #tmp_liquidacion",
        autocommit=True
    )

    ejecutar_script = MsSqlOperator(
        task_id="ejecutar_procedimiento",
        mssql_conn_id=database,
        sql=query1,
        autocommit=True
    )

    obtener_datos_db = PythonOperator(
        task_id="obtener_datos", 
        python_callable=obtener_datos
    )

    borrar_tablas = PythonOperator(
        task_id="drop_tables", 
        python_callable=drop_tables
    )
    
    borrar_temp >> ejecutar_script >> obtener_datos_db >> borrar_tablas
   
