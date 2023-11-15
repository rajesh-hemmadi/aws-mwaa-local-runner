import base64
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import sys
import time
import requests
import json
from string import punctuation
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
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.models.param import Param
from airflow.hooks.postgres_hook import PostgresHook

from airflow.exceptions import AirflowException
import io
import os
import hashlib
import logging
import fnmatch
from utils.notification import send_notification
from datetime import datetime, timedelta
from Crypto.Cipher import AES
import binascii
import asyncio
import aiohttp
from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = False



REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/titan_transactions'
GDRIVE_CONN = "gdrive_service_account"
TITAN_API_CRED = 'titan_api'
SECRET = BaseHook.get_connection(TITAN_API_CRED).password
HOST = 'https://appsecure.titantrustbank.com'
CLIENT_ID = 'PaystackPytLtdNBbQs'
CHANNEL = '2'
ENCRYPTION_DETAILS_ENDPOINT = '/TitanTrustBankESB/GetEncryptionDetails'
TOKEN_ENDPOINT = '/TitanTrustBankESB/Authentication/tokengeneration/getToken'
STATEMENT_ENDPOINT = '/TitanTrustBankESB/enquiry/StatementEnquiry/PaystackTransactionsList'

# accounts_list = [{'account_number' : '0000251250' ,'start_date': '2022-02-10'},
# {'account_number' : '0000266063' ,'start_date': '2022-03-11' }]




START_DATE = '2023-10-01'

def date_range(START_DATE,END_DATE):
    required_date = START_DATE
    while required_date <= END_DATE:
        yield required_date
        if DEBUG:
            return
        required_date = required_date + timedelta(days=1)


def get_last_max_date():
    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    sql = f"select coalesce(MAX(TO_DATE(replace(split_part(split_part(file_name,'/',4),'_',6),'.csv',''),'YYYYMMDD')),'2021-12-31'::date) as max_dt from s3_ingestions.load_process_reference  where file_name like '%titan_transactions_all_accounts%'  and processed = True"
    print(sql)
    result = redshift_hook.get_first(sql)
    if result and result[0]:
        result = result[0]
    return (result)


def decrypt_message(encryptedmessage, key, iv):
    BS = AES.block_size
    unpad = lambda s: s[:-ord(s[len(s) - 1:])]
    key = key.encode("utf-8")
    #iv = iv.encode("utf-8")
    
    #encryptedmessage = encryptedmessage.decode("utf-8")

    iv = encryptedmessage[:16]
    #raw = pad(message).encode("utf8")

    cipher = AES.new(key=key, mode=AES.MODE_CBC, iv=iv)
    #r = cipher.decrypt(encryptedmessage[16:])
    r = cipher.decrypt(encryptedmessage)
    r = binascii.hexlify(r).decode("utf-8")
    return r

def encrypt_message(message, key, iv):
    BS = AES.block_size
    pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
    key = key.encode("utf-8")
    iv = iv.encode("utf-8")
    raw = pad(message).encode("utf8")
    cipher = AES.new(key=key, mode=AES.MODE_CBC, iv=iv)
    r = cipher.encrypt(raw)
    r = binascii.hexlify(r).decode("utf-8")
    return r


def get_token():
    auth = base64.b64encode((CLIENT_ID + ":" + SECRET).encode("utf-8"))
    r = (requests.get(f'{HOST}{ENCRYPTION_DETAILS_ENDPOINT}', headers={'Authorization': auth})).headers
    print("r:",r)
    iv = r['IV']
    key = r['Key']

    encrypted_secret = encrypt_message(SECRET, key, iv)

    headers = {'Channel': CHANNEL, 'ClientID': CLIENT_ID, 'Secret': encrypted_secret,
               'Content-Type': 'application/json'}
    r = requests.get(f'{HOST}{TOKEN_ENDPOINT}', headers=headers)
    r = json.loads(r.text)
    token = r['token']
    return iv, key, token


def get_list_of_accounts_from_redshift():
    # Replace 'your_sql_query' with your actual SQL query
    sql_query = """select
         ma.account_number, ma.created_at as inception_date
         from bistaging.managedaccounts ma
         join bistaging.managedaccount_providers mp
         on ma.provider_id = mp.id
         and mp.bank_name = 'Titan Bank'
    union
         select right('000000000'+convert(varchar,account_number),10) as account_number,
         '2023-09-01 00:00:00' as inception_date
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
    return df
    





def upload_file_to_s3(**kwargs):
    try:
        #Get file specific details
        task_id = kwargs.get('task_id', 'upload_file_to_s3')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        base_name, extension = os.path.splitext(file_name_with_path)
        new_local_path_csv = local_path + '/' + base_name + '.csv'
        s3_bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
        s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
        s3_folder_name = '{}/{}'.format(s3_prefix_start, file_name_with_path)
        

        s3_upload_operator = LocalFilesystemToS3Operator(
            task_id=task_id,
            aws_conn_id=S3_CONN_ID,
            filename=new_local_path_csv,
            dest_key=s3_folder_name, #+ '/' + s3_upload_file_name_with_path,
            dest_bucket=s3_bucket_name,
            replace=True
        )

        s3_upload_operator.execute(context=None)
        os.remove('{}'.format(new_local_path_csv))


    except Exception as err:
        print(err)
        message = "Function upload_file_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)

def call_api_url(account_number,start_date,end_date,encrypted_token,iv,key,token):

    headers = {'Authorization': encrypted_token, 'ClientID': CLIENT_ID, 'Content-Type': 'application/json'}

    max_retries = 10
    retry_delay = 15  # 15 seconds
    for attempt in range(1, max_retries + 1):
        try:
            print(f'Getting statement for {account_number} between {start_date} and {end_date}')
            encrypted_token = encrypt_message(token, key, iv)
            headers = {'Authorization': encrypted_token, 'ClientID': CLIENT_ID, 'Content-Type': 'application/json'}
            data = {"accountNumber": account_number, "startDate": start_date, "endDate": end_date}
            encrypted_fields = encrypt_message(json.dumps(data), key, iv)
            fields = json.dumps({"Content": encrypted_fields, "HTTPAction": "1"})
            r = (requests.post(f'{HOST}{STATEMENT_ENDPOINT}', headers=headers, data=fields))
            print(r.text)
            
            if r.ok is True:
                if int(json.loads(r.text)['responseCode']) == 2 and json.loads(r.text)['responseMessage'] in ['Transaction list is empty','Account number is invalid','Invalid date']:
                    columns_list = ['transactiontype', 'amount', 'sessionid', 'destinationbankcode', 'narration', 'channelcode', 'channel', 'sourceaccount', 'sourceaccountname', 'sourceaccountbvn', 'destinationaccount', 'destinationaccountname', 'destinationaccountbvn', 'transactiondate', 'transactionstatusmessage', 'id', 'createdon', 'isdeleted']
                    # df.columns = ['currencyCode', 'narration', 'statusMessage', 'transactionAmount', 'transactionDate', 'transactionType', 'transaction_Type', 'id','createdOn', 'isDeleted']
                    df = pd.DataFrame(columns=columns_list)
                    df['_account_number'] = account_number
                    df['_start_date'] = start_date
                    df['_end_date'] = end_date
                    df['_row_number'] = df.index
                    with open(f"/tmp/titan_transactions_unprocessed_{datetime.strftime(datetime.strptime(start_date, '%m/%d/%Y') ,'%Y-%m-%d')}.txt",'a') as unpr:
                        unpr.write(f"{account_number}_{datetime.strftime(datetime.strptime(start_date, '%m/%d/%Y') ,'%Y-%m-%d')}_{json.loads(r.text)['responseMessage']}\n")
                    return df
                
                elif int(json.loads(r.text)['responseCode']) != 0:
                    print('Problem with response')
                    print(r.text)
                    raise ValueError
                else:
                    df = pd.DataFrame(json.loads(r.text)['transactionList'])
                    if df.size != 0:
                        df.columns = [c.lower() for c in df.columns]
                        df['_account_number'] = account_number
                        df['_start_date'] = datetime.strftime(datetime.strptime(start_date, '%m/%d/%Y') ,'%Y-%m-%d')
                        df['_end_date'] = datetime.strftime(datetime.strptime(end_date, '%m/%d/%Y') ,'%Y-%m-%d')
                        df['_row_number'] = df.index
                        return df
            else:
                raise requests.HTTPError
        except (requests.HTTPError, KeyError) as e:
            print(f"Attempt {attempt} failed with error: {e}")
            print(f"Attempted  for : {account_number} {start_date} {end_date}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)
        except Exception as err:
            print(f"Attempt {attempt} failed with error: {str(err)}")
            print(f"Attempted url is : {account_number} {start_date} {end_date}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)


def call_for_single_date(account_number,start_date,end_date):
    iv, key, token = get_token()
    encrypted_token = encrypt_message(token, key, iv)
    print(f'Date Supplied is {start_date} {end_date}')
    local_path = '/tmp'
    os.makedirs(local_path, exist_ok=True)
    try:
        transactions = call_api_url(account_number=account_number,start_date=start_date,end_date=end_date,encrypted_token=encrypted_token,iv=iv,key=key,token=token)
        if len(transactions.index) == 0:
            print(f'Data Frame is Empty For {account_number} {start_date}')
            return
        #deal with balance data
        print(transactions)
        start_date = datetime.strptime(start_date, '%m/%d/%Y') 
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%m/%d/%Y') 
        end_date = datetime.strftime(end_date, '%Y-%m-%d')
        filename_all = f"titan_transactions_all_accounts_"+start_date.replace('-','')+'_'+end_date.replace('-','')+'.csv'
        header = False
        if not os.path.exists(local_path +'/'+filename_all):
            header = True
        
        transactions.to_csv(os.path.join(local_path, filename_all), header=header,index=False,mode='a')
        task_id = 'uploading_'+filename_all
        #upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)                
        print(f"Saved file: {filename_all}")


    except Exception as err:        
        print(err)
        message = "Function call_for_single_date failed,  Could not download data from api for dates " + start_date + " and "+ end_date +" : \n" + \
            str(err)
        send_notification(type="error", message=message)
        #raise AirflowException(message)
        raise ValueError(message)

def titan_transactions_api_download_as_csv(params,**kwargs):
    accounts_df = get_list_of_accounts_from_redshift()
    prev_max_date = get_last_max_date()
    expected_run_date = (datetime.now() - timedelta(days=1)).date() #This is date
    diff_in_days = (expected_run_date - prev_max_date).days
    if pd.notna(prev_max_date) and diff_in_days >= 2:
        latest_start_date = prev_max_date + timedelta(days=1) 
    else:
        latest_start_date = expected_run_date 
    latest_end_date_should_be = expected_run_date
    print(f'Start date used is : {latest_start_date}')
    print(f'Final End date used is : {latest_end_date_should_be}')  
    
    try:
        local_path = kwargs.get('local_path', '/tmp')
        os.makedirs(local_path, exist_ok=True)
        # start_date_orig = datetime.strptime(START_DATE, '%Y-%m-%d') 
        # date_file_path = f'/tmp/titan_transactions.txt'
        # check_file = os.path.exists(date_file_path)
        # if check_file:
        #     with open(date_file_path,'r') as fil:
        #         start_date = fil.readline().strip()
        #     start_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        #     if start_date_orig > start_date:
        #         start_date = start_date_orig
        # else:
        #     start_date = start_date_orig
            
        # final_end_date = datetime.now()

        for single_date in date_range(latest_start_date,latest_end_date_should_be):
            print(f'Single Date  : {single_date}')
            start_date = datetime.strftime(single_date, '%m/%d/%Y')
            end_date = start_date
            accounts_file_path = f'/tmp/titan_transactions_{datetime.strftime(single_date, "%Y-%m-%d")}.txt'
            # if os.path.exists(accounts_file_path):
            #     os.remove(accounts_file_path)
            for index, acc in accounts_df.iterrows():
                account_number = acc['account_number']
                inception_date = acc['inception_date']
                print(f'Account Number is : {account_number}')
                print(f'Inception Date: {inception_date}')
                if single_date < inception_date.date():
                    continue
                if os.path.exists(accounts_file_path):
                    with open(accounts_file_path, 'r') as afil:
                        processed_accounts = afil.read()
                    if account_number in processed_accounts:
                        continue
                call_for_single_date(account_number=account_number,start_date=start_date,end_date=end_date)
                with open(accounts_file_path,'a') as afil:
                    afil.write(account_number +'\n')

            # with open(date_file_path,'w') as fil:
            #     fil.write(datetime.strftime(single_date, '%Y-%m-%d'))
            # break
                


    except Exception as err:
        print(err)
        message = f"Function titan_transactions_api_download_as_csv failed : \n \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


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

with DAG(dag_id="titan_transactions_api_download_as_csv",
         description="titan_transactions_api_download_as_csv",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval="15 4 * * *",
         start_date=datetime(2023, 8, 12, 8, 30),
         catchup=False,
         tags=["wema", "transactions", "download"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'initial_empty_task'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    titan_transactions_api_download_as_csv = PythonOperator(task_id='titan_transactions_api_download_as_csv',
                                  python_callable=titan_transactions_api_download_as_csv,
                                  op_kwargs={'local_path': '/tmp'},
                                  )
    dummy_start >> titan_transactions_api_download_as_csv


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
