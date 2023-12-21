import logging
from time import time
from datetime import timedelta

import awswrangler as wr
import boto3
import pandas as pd
import pathlib
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
import io
import requests


logger = logging.getLogger(__name__)


def insert_query_mysql(name_project, name_query, connection_id, params, **extra_args):
    last_inserted = None
    try:
        hook = MySqlHook(mysql_conn_id=connection_id, **extra_args)
        conn = hook.get_conn()
        logger.info(f"correct connection to {connection_id}")
        cursor = conn.cursor()

        with open(
            "dags/{}/queries/{}.sql".format(name_project, name_query), "r"
        ) as query:
            query_as_string = query.read()

        query_as_string = query_as_string % (params)

        tictrac = time()
        cursor.execute(query_as_string)
        toctrac = time()
        logger.info("time:  " + str((toctrac - tictrac) / 60))
        conn.commit()
        last_inserted = cursor.lastrowid

    except TypeError as e:
        logger.exception(e)
        return None
    finally:
        conn.close()

    return last_inserted


def execute_query_mysql(name_project, name_query, connection_id, params, **extra_args):
    rows = []
    try:
        hook = MySqlHook(mysql_conn_id=connection_id, **extra_args)
        conn = hook.get_conn()
        logger.info(f"correct connection to {connection_id}")
        cursor = conn.cursor()

        with open(
            "dags/{}/queries/{}.sql".format(name_project, name_query), "r"
        ) as query:
            query_as_string = query.read()

        query_as_string = query_as_string % (params)
        tictrac = time()
        cursor.execute(query_as_string)
        rows = cursor.fetchall()
        rows = list(rows)

        toctrac = time()
        logger.info("time:  " + str((toctrac - tictrac) / 60))
        conn.commit()

    except TypeError as e:
        logger.exception(e)
        return None
    finally:
        conn.close()

    return rows


def read_query_mysql(
    name_project, name_query, connection_id, chunksize=None, **extra_args
):
    try:
        hook = MySqlHook(mysql_conn_id=connection_id, **extra_args)
        conn = hook.get_conn()
        logger.info(f"correct connection to {connection_id}")

    except TypeError as e:
        logger.error(f"the connection to db {connection_id} is wrong")
        logger.error(e)

    try:
        query = open("dags/{}/queries/{}.sql".format(name_project, name_query), "r")
        tictrac = time()
        query_as_string = query.read()
        if chunksize:
            dfl = []
            df = pd.DataFrame()
            result = pd.read_sql(sql=query_as_string, con=conn, chunksize=chunksize)
            for chunk in result:
                dfl.append(chunk)
            df = pd.concat(dfl, ignore_index=True)
        else:
            df = pd.read_sql(sql=query_as_string, con=conn)
        toctrac = time()
        logger.info("time:  " + str((toctrac - tictrac) / 60))

    except TypeError as e:
        logger.info(e)
        return None

    finally:
        conn.close()

    return df


def read_query_mysql_by_chunk(
    name_project, name_query, connection_id, optimize_datatype=None, chunksize=400000
):
    """The data must be ordered
    The datatype conversion is from object to category,
    A string variable consisting of only a few different values.
    Converting such a string variable to a categorical variable will save some memory"""
    try:
        hook = MySqlHook(mysql_conn_id=connection_id)
        conn = hook.get_conn()
        logger.info(f"correct connection to {connection_id}")
    except Exception as e:
        logger.error(f"Error when trying to connect to {connection_id}")
        raise ValueError(f"Error when trying to connect to {connection_id} error: {e}")

    try:
        offset = 0
        count = 0
        dfl = []
        df = pd.DataFrame()
        with open(
            "dags/{}/queries/{}.sql".format(name_project, name_query), "r"
        ) as query_file:
            query_as_string = query_file.read()
        tictrac = time()
        logger.info("processing the query")
        while True:
            logger.info(f"processing the {count+1} chunk, size {chunksize}")
            chunk = pd.read_sql(
                sql=query_as_string + f" limit {offset},{chunksize}", con=conn
            )
            if optimize_datatype:
                chunk[chunk.select_dtypes(["object"]).columns] = chunk.select_dtypes(
                    ["object"]
                ).apply(lambda x: x.astype("category"))
            dfl.append(chunk)
            offset += chunksize
            logger.info("offset")
            if len(chunk) < chunksize:
                break
            del chunk
            count = count + 1
        df = pd.concat(dfl, ignore_index=True)
        toctrac = time()
        logger.info("time:  " + str((toctrac - tictrac) / 60))

        return df

    except Exception as e:
        logger.error("Error when trying to process query")
        raise ValueError(f"Error when trying to process query: {e}")

    finally:
        conn.close()


def read_query_postgresql(name_project, name_query, connection_id):
    try:
        hook = PostgresHook(postgres_conn_id=connection_id)
        conn = hook.get_conn()
        # conn = hook.get_sqlalchemy_engine()
        logger.info(f"correct connection to {connection_id}")

    except TypeError as e:
        logger.info(e)
        return None

    try:
        query = open("dags/{}/queries/{}.sql".format(name_project, name_query), "r")
        tictrac = time()
        query_as_string = query.read()
        df = pd.read_sql(sql=query_as_string, con=conn)
        toctrac = time()
        logger.info("time:  " + str((toctrac - tictrac) / 60))

    except TypeError as e:
        logger.info(e)
        return None

    finally:
        conn.close()

    return df


def df_to_s3(df, bucket, folder, filename):
    try:
        AWS_ACCESS_KEY = Variable.get("secret_aws_access_key")
    except:
        logger.error("variable secret_aws_access_key does not exist")
    try:
        AWS_SECRET_KEY = Variable.get("secret_aws_secret_key")
    except:
        logger.error("variable secret_aws_secret_key does not exist")

    try:
        my_session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        path = pathlib.Path(filename)

        if path.suffix == ".csv":
            wr.s3.to_csv(
                df=df,
                path=f"s3://{bucket}/{folder}/{filename}",
                boto3_session=my_session,
                index=False,
                decimal=",",
            )
            logger.info("file upload complete")

        elif (path.suffix == ".gzip") | (path.suffix == ".parquet"):
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{bucket}/{folder}/{filename}",
                boto3_session=my_session,
            )
            logger.info("file upload complete")

        elif path.suffix == ".xlsx":
            wr.s3.to_excel(
                df=df,
                path=f"s3://{bucket}/{folder}/{filename}",
                boto3_session=my_session,
            )
            logger.info("file upload complete")

    except Exception as e:
        logger.error(e)


def s3_to_df(bucket, folder, filename, **pandas_kwargs):
    try:
        AWS_ACCESS_KEY = Variable.get("secret_aws_access_key")
    except:
        logger.error("variable secret_aws_access_key does not exist")
    try:
        AWS_SECRET_KEY = Variable.get("secret_aws_secret_key")
    except:
        logger.error("variable secret_aws_secret_key does not exist")

    try:
        my_session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )

        path = pathlib.Path(filename)
        if path.suffix == ".csv":
            df = wr.s3.read_csv(
                path=f"s3://{bucket}/{folder}/{filename}",
                boto3_session=my_session,
                **pandas_kwargs,
            )
            return df

        elif path.suffix == ".gzip":
            df = wr.s3.read_parquet(
                path=f"s3://{bucket}/{folder}/{filename}", boto3_session=my_session
            )
            return df

        elif path.suffix == ".xlsx":
            df = wr.s3.read_excel(
                path=f"s3://{bucket}/{folder}/{filename}",
                boto3_session=my_session,
                **pandas_kwargs,
            )
            return df

    except Exception as e:
        logger.error(e)


def read_mongodb(connection_id, db, collection, filter=None, projection=None):
    try:
        mongodb = Variable.get(connection_id)
        client = MongoClient(mongodb, unicode_decode_error_handler="ignore")
    except:
        logger.error("error in connection to mongo")

    db_imports = client["{}".format(db)]
    list_imports = db_imports["{}".format(collection)]
    cursor_imports = list_imports.find(filter, projection)
    mongo_imports = list(cursor_imports)
    return mongo_imports


def return_mongodb(connection_id):
    try:
        mongodb = Variable.get(connection_id)
        client = MongoClient(mongodb)
        logger.info("correct connection to mongo")
    except:
        logger.error("error in connection to mongo")

    return client


def get_env():
    try:
        ENV = Variable.get("ENV")
    except:
        logger.error("variable ENV does not exist")
        ENV = ""
    return ENV


def get_variable(variable):
    try:
        var = Variable.get(variable)
        return var

    except Exception as e:
        logger.error(e)


def hour_changer(schedule_string, its_utc3=False):
    flag_sch = get_variable("flag_hour")

    if type(schedule_string) is int:
        if flag_sch == "winter" or (its_utc3 and flag_sch == "summer"):
            return schedule_string
        elif flag_sch == "summer":
            DELTA = -1
        else:
            DELTA = int(flag_sch)
        new_hour = timedelta(hours=schedule_string) + timedelta(hours=DELTA)
        return int(new_hour.seconds / 3600)

    if (
        flag_sch == "winter"
        or (its_utc3 and flag_sch == "summer")
        or schedule_string.split()[1] == "*"
    ):
        return schedule_string
    elif flag_sch == "summer":
        DELTA = -1
    else:
        DELTA = int(flag_sch)

    list_sch = schedule_string.split()
    hours = list_sch[1].split(",")

    if len(hours) > 1:
        new_hours = [(timedelta(hours=int(x)) + timedelta(hours=DELTA)) for x in hours]
        new_hours = [str(int(x.seconds / 3600)) for x in new_hours]
        new_hour = ",".join(new_hours)
    else:
        aux = timedelta(hours=int(hours[0])) + timedelta(hours=DELTA)
        aux = str(int(aux.seconds / 3600))
        new_hour = str(aux) if len(str(aux)) > 1 else "0" + str(aux)

    list_result = []
    for i, elem in enumerate(list_sch):
        if i != 1:
            list_result.append(elem)
        else:
            list_result.append(new_hour)
    new_string = " ".join(list_result)
    return new_string


def get_file_api_to_dataframe(url, **csv_options):
    token = get_variable("secret_file_api_token")

    data = io.BytesIO()
    res = requests.get(url, headers={"Authorization": token}, timeout=1800)
    data.write(res.content)
    data.seek(0)

    filename = url.split("download?name=")[1]
    path = pathlib.Path(filename)
    if path.suffix == ".csv":
        df = pd.read_csv(data, encoding="utf8", **csv_options)
    elif path.suffix == ".xlsx":
        df = pd.read_excel(data)
    return df
