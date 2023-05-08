from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
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
from airflow.exceptions import AirflowException
import io
import os
import logging
from utils.notification import send_notification


from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = False

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files'
GDRIVE_CONN = "gdrive_service_account"


mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}


def test_concept(**kwargs):
    # items = get_files_from_gdrive_parent_subfolders('1UhZmufgxPBJTalNjvnfvIimHOmOF8gEh','text/csv',1)
    # items = get_files_from_gdrive_parent_subfolders(parent_folder_id='1UZk2JArK5_IzrgaI-PAK11W11tNsHulC',mimetype='text/csv',subfolder_level=1)
    # items = get_files_from_gdrive_parent_subfolders(parent_folder_id='1_O4sf7kGL4LFmIiMcP6a9IbDPq0FZn5U',mimetype='text/csv',subfolder_level=1)
    items = get_files_from_gdrive_parent_subfolders(parent_folder_id='1Rq0GdBE81CmR-IbLmqcenrXGiicTxFQO',mimetype='text/csv',subfolder_level=0)
    for item in items:
        print(item)

def get_files_from_gdrive_parent_subfolders(parent_folder_id, mimetype, subfolder_level=None):
    drive_hook = GoogleDriveHook(gcp_conn_id=GDRIVE_CONN)
    # Build the Drive API client using the credentials from the connection
    drive_service = build('drive', 'v3', credentials=drive_hook.get_credentials())

    # Function to retrieve files within a folder and its subfolders recursively
    def retrieve_files(folder_id, current_level, page_token=None):
        files = []
        query = f"'{folder_id}' in parents and trashed = false"

        if mimetype:
            query += f" and (mimeType = '{mimetype}' or mimeType = 'application/vnd.google-apps.folder')"

        response = drive_service.files().list(
            q=query,
            fields='files(id, name, parents, mimeType)',
            pageToken=page_token
        ).execute()

        items = response.get('files', [])
        files += items

        if subfolder_level is not None and current_level >= subfolder_level:
            return files

        subfolders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']
        print(f"Subfolders in folder '{folder_id}': {len(subfolders)}")

        for subfolder in subfolders:
            subfolder_id = subfolder['id']
            print(f"Retrieving files in subfolder '{subfolder_id}' at level {current_level + 1}")
            subfolder_files = retrieve_files(subfolder_id, current_level + 1)
            files += subfolder_files

        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
            print(f"Retrieving next page of files in folder '{folder_id}'")
            next_page_files = retrieve_files(folder_id, current_level, page_token=next_page_token)
            files += next_page_files

        return files

    # Retrieve files from the parent folder and its subfolders
    print(f"Retrieving files from parent folder '{parent_folder_id}'")
    files = retrieve_files(parent_folder_id, 0)

    # Create a list of files with their IDs, names, and complete file paths
    file_list = []
    for file in files:
        if file['mimeType'] == 'application/vnd.google-apps.folder': #We do not need folders
            continue
        file_id = file['id']
        file_name = file['name']
        parents = file.get('parents', [])

        # Retrieve the complete file path
        file_path = file_name
        for parent_id in parents:
            parent_name = drive_service.files().get(fileId=parent_id, fields='name').execute().get('name', '')
            file_path = parent_name + '/' + file_path

        # Append the file ID, name, and complete file path to the list
        file_list.append({'id': file_id, 'name': file_name, 'path': file_path})

    return file_list





default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}
with DAG(dag_id="quick_test",
         description="quick_test",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["gdrive", "ingestions"]
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'}

                                  )
    dummy_start >> test_concept 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
