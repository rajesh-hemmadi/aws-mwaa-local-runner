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
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
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
    path = r'/tmp/Unified Payments report/202304/PAYSTACK PAYMENTS LIMITED_25-Apr-2023.xlsx'
    source_df = pd.read_excel(path, sheet_name='DETAILS', skiprows=10,dtype=str)
    print(source_df.head())
    #retrun_data_from_stored_procedure_as_df(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_get_list_of_processed_files_for_unique_load_name', params={
    #                                                               'p_unique_load_name': 'paystackharmonyredshift.gdrive_ingestions_test.pat_fone_paystack_payments__xlsx__1UbgPE5JzXAdBxuodIEgp4AYeveafZs4h__PAYSTACK PAYMENTS', 'rs_out': 'rs_out'})

# def test_concept(**kwargs):
#     data = {
#         'unique_load_name': ['paystackharmonyredshift.gdrive_ingestions_test.isw_pos_reports__csv__1UaJnIK2TglO6kB_hZy8Shz0xREDI0Ole'] * 33,
#         'source_column_name': [
#              'DateTime', 'Currency_Name',
#             'Local_Date_Time', 'Terminal_ID', 'Merchant_ID', 'Merchant_Name_Location', 'STAN', 'PAN', 'Message_Type',
#             'From_Account_ID', 'Merchant_Account_Nr', 'Merchant_Account_Name', 'From_Account_Type', 'tran_type_description',
#             'Response_Code_description', 'Tran_Amount_Req', 'Tran_Amount_Rsp', 'Surcharge', 'Amount_Impact',
#             'merch_cat_category_name', 'Settlement_Impact', 'Settlement_Impact_Desc', 'Merchant_Discount', 'Merchant_Receivable',
#             'Auth_ID', 'Tran_ID', 'Retrieval_Reference_Nr', 'Totals_Group', 'Transaction_Status', 'Region',
#             'Transaction_Type_Impact', 'Message_Type_Desc', 'trxn_category'
#         ],
#         'target_column_name': [
#              'DateTime', 'Currency_Name',
#             'Local_Date_Time', 'Terminal_ID', 'Merchant_ID', 'Merchant_Name_Location', 'STAN', 'PAN', 'Message_Type',
#             'From_Account_ID', 'Merchant_Account_Nr', 'Merchant_Account_Name', 'From_Account_Type', 'tran_type_description',
#             'Response_Code_description', 'Tran_Amount_Req', 'Tran_Amount_Rsp', 'Surcharge', 'Amount_Impact',
#             'merch_cat_category_name', 'Settlement_Impact', 'Settlement_Impact_Desc', 'Merchant_Discount', 'Merchant_Receivable',
#             'Auth_ID', 'Tran_ID', 'Retrieval_Reference_Nr', 'Totals_Group', 'Transaction_Status', 'Region',
#             'Transaction_Type_Impact', 'Message_Type_Desc', 'trxn_category'
#         ]
#     }

#     df = pd.DataFrame(data)
#     print(df)
#     prepare_files_to_upload_to_s3(task_id=1, folder_id=1, file_name_with_path='POS report/202305/PAY_STACK1_Pos_Acquired_Detail_Report_DR_2023_05_24_190127 (1).csv', local_path='/tmp',top_rows_to_skip=0,column_metadata_df=df)



def prepare_files_to_upload_to_s3(**kwargs):
    try:
        column_metadata_df = kwargs.get('column_metadata_df')
        task_id = kwargs.get('task_id', 'prepare_files_to_upload')
        folder_id = kwargs.get('folder_id')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        file_extension = kwargs.get('file_extension','csv')
        source_file_delimiter = kwargs.get('source_file_delimiter',',')
        source_sheet_name = kwargs.get('source_sheet_name','Sheet1')
        only_file_name = file_name_with_path.split('/')[-1]
        top_rows_to_skip = kwargs.get('top_rows_to_skip')
        final_local_path = local_path + '/' + file_name_with_path #os.path.dirname(file_name_with_path)
        base_name, extension = os.path.splitext(final_local_path)
        new_local_path = base_name + '.csv_tmp'
        new_local_path_csv = base_name + '.csv'
        #read source file
        if file_extension == 'csv':
            source_df = pd.read_csv(final_local_path,skiprows=top_rows_to_skip,sep=source_file_delimiter)
        elif file_extension == 'xlsx':
            source_df = pd.read_excel(final_local_path, sheet_name=source_sheet_name, skiprows=top_rows_to_skip)
        else:
            print(f'Unknown file extension {file_extension}')
            return False
        #from source df prepare output df and file
        # Select only the desired columns from the source_df
        output_df = source_df[column_metadata_df['source_column_name']]
        #reindex for better readability in order of column
        output_df = output_df.reindex(columns=column_metadata_df['source_column_name'])
        # Rename the columns to target_column_name
        output_df.columns = column_metadata_df['target_column_name'].tolist()
        # Write the output DataFrame to a new CSV file
        output_df.to_csv(new_local_path, index=False, quoting=1)

        os.remove(final_local_path)
        os.rename(new_local_path,new_local_path_csv)

    except Exception as err:
        print(err)
        message = "Function prepare_files_to_upload_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)

def retrun_data_from_stored_procedure_as_df(**kwargs):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        param_str = ", ".join([f"'{value}'" if not isinstance(
            value, int) else f"{value}" for value in params.values()])
        result_name = param_str.split(',')[-1].replace("'", "")

        # columns = ['id','source_folder_path','source_file_type','source_sheet_name','is_active','source_folder_name','target_table','target_db','target_schema','unique_load_name','file_last_extract_datetime']
        # Define the SQL queries to execute
        sql_queries = [
            "CALL {}.{}({});".format(schema_name, sp_name, param_str),
            f"FETCH ALL FROM {result_name};"
        ]

        results = []
        column_names = []
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_query in sql_queries:
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    if result:
                        results.append(result)
                        # Extract the column names
                        column_names.append([col_desc[0]
                                            for col_desc in cursor.description])

        # Convert the results to a pandas dataframe
        df = pd.DataFrame(results[1], columns=column_names[1])

        print(df)
        return df

    except Exception as err:
        print(err)
        message = "Function retrun_data_from_stored_procedure_as_df failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

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
