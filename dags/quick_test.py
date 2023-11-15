from typing import Any, Callable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.hooks.base import BaseHook
from airflow.providers.google.suite.operators.sheets import GSheetsHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator, S3CopyObjectOperator
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator
from googleapiclient.discovery import build
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook as BaseSFTPHook
from airflow.models.param import Param
from airflow.hooks.postgres_hook import PostgresHook

from airflow.exceptions import AirflowException
import io, stat
import os, sys, re
import logging
import fnmatch
import glob
from utils.notification import send_notification
from stat import S_ISDIR, S_ISREG

from airflow.models import XCom
#from utils.pre_processing_scripts import ghipssGenerateDateFolders, remove_preambles
from utils.pre_processing_scripts import *


from datetime import datetime, timedelta
DEBUG = False

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files'
GDRIVE_CONN = "gdrive_service_account"
SFTP_ACCOUNT = 'sftp_absa_gh'

mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}

def get_list_of_accounts_from_redshift():
    # Replace 'your_sql_query' with your actual SQL query
    sql_query = """select
             ma.account_number
         from bistaging.managedaccounts ma
         join bistaging.managedaccount_providers mp
         on ma.provider_id = mp.id
         and mp.bank_name = 'Titan Bank'
    union
         select right('000000000'+convert(varchar,account_number),10)
         
         from finance.paystack_bankaccount_master
         where bank = 'Titan Trust' and
         (account_type !='Managed' or account_number =201028)"""

    # Use the RedshiftSQLHook to run the query
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)

    # Fetch the results into a Pandas DataFrame
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=columns)

    # Do something with your DataFrame, maybe save it to a file or process it further
    print(df.head())
    return df

def test_concept(params,**kwargs):
    get_list_of_accounts_from_redshift()

default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

default_params ={
         "override_dates": 'N',
         "start_date": '1901-01-01',
         "end_date": '1901-01-01'
     }

with DAG(dag_id="quick_test",
         description="quick_test",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["gdrive", "ingestions"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'},
                                  )
    dummy_start >> test_concept 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
