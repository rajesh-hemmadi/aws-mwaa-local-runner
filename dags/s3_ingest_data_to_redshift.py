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
REDSHIFT_SCHEMA = "s3_ingestions"
S3_CONN_ID = 'paystack_s3'
# S3_BUCKET_NAME = 'paystack-datalake'
# S3_PREFIX_START = 'gdrive_files'
GDRIVE_CONN = "gdrive_service_account"


mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}

def is_single_ascii(character):
    # Get the Unicode code point of the character
    code_point = ord(character)

    # Check if the code point is within the ASCII range (0-127)
    return code_point >= 0 and code_point <= 127


def redshift_call_stored_procedure(**kwargs):
    try:
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        if 'params' not in kwargs:
            params = None
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        # Call stored procedure using the connection
        if params is None:
            redshift_hook.run("CALL {}.{}();".format(schema_name, sp_name))
            # redshift_hook.run(sql_query)
        else:
            # param_str = ", ".join([f"'{key}' => '{value}'" if not isinstance(value, int) else f"'{key}' => {value}" for key, value in params.items()])
            # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
            param_str = ", ".join([f"'{value}'" if not isinstance(
                value, int) else f"{value}" for value in params.values()])
            sql_query = "CALL {}.{}({});".format(
                schema_name, sp_name, param_str)
            print(sql_query)
            redshift_hook.run(sql_query)
    except Exception as err:
        print(err)
        message = "Function redshift_call_stored_procedure failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def get_s3_metadata_from_metadataSheet():
    try:
        # Specify the file path and sheet name of the Google Sheet
        spreadsheet_id = '1v_vz4gNFokq5i_HhXrE8ExURlouna7BxnYEjHVVBBHY'
        file_metadata_sheet_name = "file_metadata!A:M"
        file_metadata_columns = ['id', 'source_folder_path', 'source_file_type', 'source_file_delimiter', 'source_sheet_name', 'top_rows_to_skip',
                                 'pre_processing_script', 'target_table', 'is_active',   'target_db', 'target_schema', 'unique_load_name']
        column_metadata_sheet_name = "column_metadata!A:J"
        column_metadata_columns = ['id', 'file_metadata_id', 'source_column_name', 'target_column_name',
                                   'target_column_order', 'target_data_type', 'target_length', 'source_format', 'target_format']

        # Initialize the Google Sheets client using the gsheets_service_account connection
        hook = GSheetsHook(gcp_conn_id=GDRIVE_CONN)
        # Read all data from the sheet and store it in a list of lists
        file_metadata = hook.get_values(
            spreadsheet_id=spreadsheet_id, range_=file_metadata_sheet_name)
        df_file_metadata = pd.DataFrame(
            file_metadata[1:], columns=file_metadata[0])
        df_file_metadata = df_file_metadata[file_metadata_columns]
        df_file_metadata = df_file_metadata.astype(
            str)  # convert all columns types to string
        df_file_metadata['id'].replace('', np.nan, inplace=True)
        df_file_metadata.dropna(subset=['id'], inplace=True)
        print(df_file_metadata)
        load_data_to_redshift_temp_temple(
            df_file_metadata, 'file_metadata_temp')

    except Exception as err:
        print(err)
        message = "Error Loading File Metadata : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

    try:
        column_metadata = hook.get_values(
            spreadsheet_id=spreadsheet_id, range_=column_metadata_sheet_name)
        df_column_metadata = pd.DataFrame(
            column_metadata[1:], columns=column_metadata[0])
        df_column_metadata = df_column_metadata[column_metadata_columns]
        df_column_metadata = df_column_metadata.astype(
            str)  # convert all columns types to string
        df_column_metadata['id'].replace('', np.nan, inplace=True)
        df_column_metadata.dropna(subset=['id'], inplace=True)
        print(df_column_metadata)

        load_data_to_redshift_temp_temple(
            df_column_metadata, 'column_metadata_temp')
    except Exception as err:
        print(err)
        message = "Error Loading Column Metadata : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def load_data_to_redshift_temp_temple(data_frame, target_table):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        engine = redshift_hook.get_sqlalchemy_engine()
        data_frame.to_sql(target_table, con=engine,
                          schema=REDSHIFT_SCHEMA, index=False, if_exists='replace')
    except Exception as err:
        print(err)
        message = "Function load_data_to_redshift_temp_temple failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)
def read_s3_csv_first_row(bucket_name, key, source_file_delimiter=',',top_rows_to_skip=1):
    try:
        print(bucket_name)
        print(key)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        obj = s3_hook.get_key(key, bucket_name)
        print('Object')
        print(obj)
        line_number = 1
        for data in obj.get()['Body']._raw_stream:
            if line_number == top_rows_to_skip:
                required_header_line =  data.decode("utf-8")
                break
            line_number += 1
        print('Required Line')
        print(required_header_line)
        print('Required Header Line')
        print(required_header_line)
        required_header_line = required_header_line.split(source_file_delimiter)
        required_header_line = [s.strip('\ufeff\r\n') for s in required_header_line]
        return required_header_line
    except Exception as err:
        print(err)
        message = "Function read_s3_csv_first_row failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)
    
def assign_redshift_sp_output_to_df(**kwargs):
    try:
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        result_name = kwargs.get('result_name', 'rs_out')
        ti = kwargs['ti']

        # columns = ['id','source_folder_path','source_file_type','source_sheet_name','is_active','source_folder_name','target_table','target_db','target_schema','unique_load_name','file_last_extract_datetime']
        # Define the SQL queries to execute
        sql_queries = [
            "CALL {}.{}('{}');".format(schema_name, sp_name, result_name),
            f"FETCH ALL FROM {result_name};"
        ]
        # Instantiate a RedshiftSQLHook to execute the query
        redshift_hook = RedshiftSQLHook(
            redshift_conn_id=REDSHIFT_CONN_ID, auto_commit=False)
        # Execute each SQL query separately
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

        # Print the dataframe
        print(df['unique_load_name'])
        # for index, row in df.iterrows():
        #     print(row['file_last_extract_datetime'])

        json_df = df.to_json(date_format='iso', orient='records')
        # print(json_df)

        ti.xcom_push(key='s3_unique_load_details', value=json_df)
    except Exception as err:
        print(err)
        message = "Function assign_redshift_sp_output_to_df failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)
    
def read_column_metadata_final_produce_df(**kwargs):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        param_str = ", ".join([f"'{value}'" if not isinstance(
            value, int) else f"{value}" for value in params.values()])
        result_name = param_str.split(',')[-1].replace("'", "")
        print("CALL {}.{}({});".format(schema_name, sp_name, param_str))
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
                    print('result is below')
                    print(result)
                    if result:
                        results.append(result)
                        # Extract the column names
                        column_names.append([col_desc[0]
                                            for col_desc in cursor.description])

        data = results[1]
        column_list = [
            element for nested_list in data for element in nested_list]
        return column_list
    except Exception as err:
        print(err)
        message = "Function read_column_metadata_final_produce_df failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)




def return_first_cell_data_from_redshift_stored_procedure(**kwargs):
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
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_query in sql_queries:
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    print('result is below')
                    print(result)
                    if result:
                        results.append(result)

        data = results[1][0][0]
        return data

    except Exception as err:
        print(err)
        message = "Function return_load_reference_id failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)




def load_files_from_S3_to_RedShift_one_file_at_a_time(**kwargs):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    dag_id = kwargs['dag_id']
    run_id = kwargs['run_id']

    unique_load_name = kwargs['unique_load_name']
    # source_folder_path = kwargs['source_folder_path']
    s3_bucket = kwargs['s3_bucket']
    s3_prefix = kwargs['s3_path']
    source_file_type = kwargs['source_file_type']
    source_file_delimiter = kwargs['source_file_delimiter']
    top_rows_to_skip = kwargs['top_rows_to_skip']
    target_table = kwargs['target_table']
    target_table_temp = target_table + '_temp'
    target_schema = kwargs['target_schema']
    file_name = kwargs['file_name']
    fil = s3_prefix +file_name
    # Get Column list from redshift table
    required_column_list = read_column_metadata_final_produce_df(schema_name='s3_ingestions', sp_name='s3_process_read_column_metadata_final', params={
                                                                 'unique_load_name': unique_load_name, 'rs_out': 'rs_out'})
    file_columns = read_s3_csv_first_row(s3_bucket, fil, source_file_delimiter,top_rows_to_skip)
    for col_name in required_column_list:
        if col_name not in file_columns:
            print('Source Column Name ' + col_name + ' Not present in file ' + fil)
            print('There should be option to send email here')
            current_date_time = datetime.now()
            redshift_call_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_insert_update_load_process_reference',
                                           params={
                                               'p_unique_load_name': unique_load_name,
                                               'p_file_name': file_name,
                                               'p_file_last_extract_datetime': current_date_time,
                                               'p_processed': 0,
                                               'p_date_process_started': current_date_time,
                                               'p_date_process_last_updated': current_date_time,
                                               'p_airflow__dag_id': dag_id,
                                               'p_airflow__run_id': run_id,
                                               'p_airflow__external_trigger': 1,
                                               'p_airflow__start_date': current_date_time,
                                               'p_airflow__execution_date': current_date_time,
                                               'p_process_current_status': 'Column Name ' + col_name + ' Not present in file ' + file_name,
                                               'p_first_time_check':0})
            return
    temp_column_list = ",".join(file_columns)
 
    redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='s3_process_create_temp_table_with_columnslist', params={
                                    'p_unique_load_name': unique_load_name,'columns_list' : temp_column_list})



    # Copy data to temp table
    print('Copying File ' + fil)
    load_data_task = S3ToRedshiftOperator(
        task_id='load_data_to_redshift',
        s3_bucket=s3_bucket,
        s3_key=fil,
        schema='s3_ingestions',
        table=target_table_temp,
        copy_options=["CSV", f"DELIMITER '{source_file_delimiter}'", f"IGNOREHEADER {top_rows_to_skip}", "IGNOREBLANKLINES"],
        aws_conn_id=S3_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID
    )


    current_date_time = datetime.now()
    redshift_call_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_insert_update_load_process_reference',
                                                       params={
                                                           'p_unique_load_name': unique_load_name,
                                                           'p_file_name': file_name,
                                                           'p_file_last_extract_datetime': current_date_time,
                                                           'p_processed': 0,
                                                           'p_date_process_started': current_date_time,
                                                           'p_date_process_last_updated': current_date_time,
                                                           'p_airflow__dag_id': dag_id,
                                                           'p_airflow__run_id': run_id,
                                                           'p_airflow__external_trigger': 1,
                                                           'p_airflow__start_date': current_date_time,
                                                           'p_airflow__execution_date': current_date_time,
                                                           'p_process_current_status': 'Data Loading from S3 to Redshift in progress',
                                                           'p_first_time_check':0
                                                           })
    load_data_task.execute(context=None)
    p_load_process_reference_id = return_first_cell_data_from_redshift_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_get_load_reference_id', params={
                                                                   'p_unique_load_name': unique_load_name, 'p_file_name': file_name, 'rs_out': 'rs_out'})
    redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='s3_process_read_temp_load_target', params={
                                   'p_unique_load_name': unique_load_name, 'p_schema_name': target_schema, 'p_target_table': target_table, 'p_load_process_reference_id': p_load_process_reference_id})
    current_date_time = datetime.now()
    redshift_call_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_insert_update_load_process_reference',
                                                       params={
                                                           'p_unique_load_name': unique_load_name,
                                                           'p_file_name': file_name,
                                                           'p_file_last_extract_datetime': current_date_time,
                                                           'p_processed': 1,
                                                           'p_date_process_started': current_date_time,
                                                           'p_date_process_last_updated': current_date_time,
                                                           'p_airflow__dag_id': dag_id,
                                                           'p_airflow__run_id': run_id,
                                                           'p_airflow__external_trigger': 1,
                                                           'p_airflow__start_date': current_date_time,
                                                           'p_airflow__execution_date': current_date_time,
                                                           'p_process_current_status': 'Process Completed',
                                                           'p_first_time_check':0})


def list_and_load_files_from_s3_to_redshift(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        dag_id = kwargs['dag'].dag_id
        run_id = kwargs['run_id']
        ti = kwargs['ti']
        json_df = ti.xcom_pull(key='s3_unique_load_details')
        df = pd.read_json(json_df)

        for index, row in df.iterrows():
            # last_processed_datetime = (datetime.now() - timedelta(days=7)).isoformat() + 'Z'
            s3_uri = row['source_folder_path']
            s3_bucket = s3_uri.replace('s3://','').split('/')[0]
            s3_path = s3_uri.replace('s3://','').split('/',1)[1]
            file_extension = row['source_file_type']
            top_rows_to_skip = row['top_rows_to_skip']
            if top_rows_to_skip is None or top_rows_to_skip == 0 or top_rows_to_skip == '':
                top_rows_to_skip = 1
            else:
                top_rows_to_skip = top_rows_to_skip + 1
            print('Top rows to Skip : ' , top_rows_to_skip)
            source_file_delimiter = row['source_file_delimiter']
            if len(source_file_delimiter) > 1:
                is_delimiter_single_ascii = False
            else:
                is_delimiter_single_ascii = is_single_ascii(source_file_delimiter)
            if not is_delimiter_single_ascii:
                # We need to introduce function to convert this multi delimiter to single delimiter as COPY command wont handle multi delimiter
                # This process needs to happen once file gets downloaded to airflow server
                # Another approach to handle it is we load data as one row and delimit in redshift using pure sql
                # For now we will print message and continue to next file in loop
                print(f"We cannot process this delimiter : {source_file_delimiter}")
                continue 
                
            mimetype = mimetype_dict.get(file_extension)
            if mimetype is None:
                message = f"No MimeType found for File Extension '{file_extension}'"
                logging.info(message)
                # May be send an email
                # continue to next load
                send_notification(type="info", message=message)
                continue

            # get list of files
            items = s3_hook.list_keys(
                s3_bucket, prefix=s3_path)
            print('All files')
            print(items)
            items = [key for key in items if key != f'{s3_path}']
            print(items)
            

            # Iterate through the list of files and print their names and modified times
            if DEBUG:  # When set debug deal with only one file rather than all files
                items = items[:1]

            for item in items:
                current_date_time = datetime.now()
                file_name = item.split('/')[-1]
                # file_parent_folder_id = item['parent_folder_id']
                # task_id = f"Downloading_file_{file_name.split('/')[-1].replace(' ','_')}"
                # print(f"{item['name']} modified at {item['modifiedTime']}")
                #if we have already processed the same file based on unique_load_name and file_name and processed = True then we skip and move to next file
                v_is_file_processed = return_first_cell_data_from_redshift_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_check_if_file_processed', params={
                                                                   'p_unique_load_name': row['unique_load_name'], 'p_file_name': file_name, 'rs_out': 'rs_out'})
                #if file is already processed continue to next file
                if v_is_file_processed:
                    continue
                
                redshift_call_stored_procedure(schema_name='s3_ingestions', sp_name='s3_process_insert_update_load_process_reference',
                                               params={
                                                   'p_unique_load_name': row['unique_load_name'],
                                                   'p_file_name': file_name,
                                                   'p_file_last_extract_datetime': current_date_time,
                                                   'p_processed': 0,
                                                   'p_date_process_started': current_date_time,
                                                   'p_date_process_last_updated': current_date_time,
                                                   'p_airflow__dag_id': dag_id,
                                                   'p_airflow__run_id': run_id,
                                                   'p_airflow__external_trigger': 1,
                                                   'p_airflow__start_date': current_date_time,
                                                   'p_airflow__execution_date': current_date_time,
                                                   'p_process_current_status': 'File Transfer In Progress',
                                                   'p_first_time_check':1})
              
                load_files_from_S3_to_RedShift_one_file_at_a_time(dag_id=dag_id, run_id=run_id, unique_load_name=row['unique_load_name'], source_file_type=file_extension,source_file_delimiter=source_file_delimiter,top_rows_to_skip=top_rows_to_skip, target_table=row['target_table'],
                                                                  target_schema=row['target_schema'], file_name=file_name,s3_bucket=s3_bucket,s3_path=s3_path)
    except Exception as err:
        print(err)
        message = "Function list_files_to_transfer failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)



def test_concept(**kwargs):
    print(kwargs.get('message'))


default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}
with DAG(dag_id="s3_ingest_data_to_redshift",
         description="Ingests the data from S3 into redshift",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["s3", "ingestions"]
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'}

                                  )
    get_s3_metadata_from_metadataSheet = PythonOperator(task_id='get_s3_metadata_from_metadataSheet',
                                                            python_callable=get_s3_metadata_from_metadataSheet
                                                            )

    run_sp_to_update_metadata_final_table = PythonOperator(task_id='run_sp_to_update_metadata_final_table',
                                                           python_callable=redshift_call_stored_procedure,
                                                           op_kwargs={
                                                               'schema_name': REDSHIFT_SCHEMA, 'sp_name': 's3_process_insert_update_metadata'}
                                                           )
    run_sp_to_create_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_create_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 's3_process_create_update_target_table_based_on_metadata'},

                                                                            )
    get_s3_unique_load_details_from_metadata_table = PythonOperator(task_id='get_s3_unique_load_details_from_metadata_table',
                                                                 python_callable=assign_redshift_sp_output_to_df,
                                                                 op_kwargs={
                                                                     'schema_name': REDSHIFT_SCHEMA, 'sp_name': 's3_process_read_file_metadata_final', 'result_name': 'rs_out'}

                                                                 )
    list_and_load_files_from_s3_to_redshift = PythonOperator(task_id='list_and_load_files_from_s3_to_redshift',
                                                              python_callable=list_and_load_files_from_s3_to_redshift,
                                                              op_kwargs={
                                                                  'run_id': '{{ run_id }}'},
                                                              provide_context=True
                                                              )
    
    dummy_start >> test_concept >> get_s3_metadata_from_metadataSheet >> run_sp_to_update_metadata_final_table >> \
    run_sp_to_create_target_tables_based_on_metadata_table >> get_s3_unique_load_details_from_metadata_table >> \
    list_and_load_files_from_s3_to_redshift

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()