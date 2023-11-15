
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import os
import io
import sys
import pandas as pd
import numpy as np
import redshift_connector
import logging
import linecache
from xlrd import open_workbook, XLRDError
import msoffcrypto
from boto.s3.connection import S3Connection, Bucket, Key
import psycopg2
import shutil
import xlwings as xw
from html2excel import ExcelParser
from utils.notification import send_notification
from airflow.exceptions import AirflowException




# Create a logger
logger = logging.getLogger('ingest_statements_docker_script')

# Create a JSON formatter
json_formatter = logging.Formatter('{"service": "%(name)s", "created": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')

# Create a handler and set the formatter
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(json_formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)

# Set the logging level (optional)
logger.setLevel(logging.DEBUG)



REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/titan_transactions'
GDRIVE_CONN = "gdrive_service_account"

rs_username = BaseHook.get_connection(REDSHIFT_CONN_ID).login
rs_password = BaseHook.get_connection(REDSHIFT_CONN_ID).password
gdrive_cred = json.loads(BaseHook.get_connection(GDRIVE_CONN).extra)
gdrive_cred = json.loads(gdrive_cred["extra__google_cloud_platform__keyfile_dict"])
s3_access_key = BaseHook.get_connection(S3_CONN_ID).login
s3_access_secret = BaseHook.get_connection(S3_CONN_ID).password

configs = {
    'Nigeria': {
     'files_list_spreadsheet_id' :'1ZjsRW9ggyGhTlAIdMZcjb1gxpZSN9DRS90E8kw56CV0' , 
     'file_type_to_consider' : ['csv','xlsx','xls','xlsb'],
     'text_to_exclude_from_file_path':['archive','ignore','not in use','balance','closing bal','statement_enquiry'],
     'accounts_to_exclude' : ['1456294137'], 
     'files_to_exclude' : ['20220101-20220105 1456294144.xlsx']
    },
     #'Ghana' :{'files_list_spreadsheet_id' :''}
     }

DEBUG = False



def statements_tag_combine_transactions(params,**kwargs):
    try:
        logger.info('Operation Started')
        con=psycopg2.connect(dbname= 'paystackharmonyredshift', host='paystackharmony-cluster.csptbdy4xa4g.eu-west-1.redshift.amazonaws.com', 
            
            #port= '5439', user= 'niel_dv', password= secrets.secrets.redshift_password)
            #conn = S3Connection('AKIAXAYWUXO2RZTD3THK',secrets.secrets.aws_secretkey)
            port= '5439', user= rs_username, password= rs_password)
        conn = S3Connection(s3_access_key,s3_access_secret)
        cur = conn.cursor()
        cur.execute("truncate table sftp_ingestions_test.bank_wrk_transactions_stage;")	
        con.commit()	
        logger.info("Merging transactions")	
        cur.execute("call sftp_ingestions_test.merge_bank_transactions();")		
        con.commit()
        cur.execute("truncate table sftp_ingestions_test.bank_wrk_transactions_import;")	
        con.commit()
        cur.execute("truncate table sftp_ingestions_test.bank_wrk_transactions_stage;")
        con.commit()
        logger.info("Tagging transactions")
        cur.execute("call sftp_ingestions_test.load_tag_transactions();")	
        con.commit()
        logger.info("Calculating Access Balances")	#NDV 20230308 I THINK MAYBE ACCESS BALANCES ARE NOW CORRECT IN THE FILE, SO REMOVING THIS FOR NOW TO CHECK
        cur.execute("call sftp_ingestions_test.calculate_access_balances();")	
        con.commit()
        logger.info("Update Kuda Filerownumbers")
        cur.execute("call sftp_ingestions_test.update_kuda_frn();")
        con.commit()
        logger.info("Combining transactions")
        cur.execute("call sftp_ingestions_test.load_combine_transactions();")
        con.commit()
        logger.info('Operation Completed')
    except Exception as err:
        print(err)
        message = f"Function statements_tag_combine_transactions failed : \n \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)




default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

default_params ={}

with DAG(dag_id="statements_tag_combine_transactions",
         description="statements_tag_combine_transactions",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="15 */18 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["monthly", "statements","tag","combine"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    statements_tag_combine_transactions = PythonOperator(task_id='statements_tag_combine_transactions',
                                  python_callable=statements_tag_combine_transactions,
                                  op_kwargs={'message': 'Test Concept'},
                                  )
    dummy_start >> statements_tag_combine_transactions 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
