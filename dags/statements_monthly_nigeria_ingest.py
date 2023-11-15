
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
GDRIVE_CONN = "gdrive_service_account"

rs_username = BaseHook.get_connection(REDSHIFT_CONN_ID).login
rs_password = BaseHook.get_connection(REDSHIFT_CONN_ID).password
gdrive_cred = json.loads(BaseHook.get_connection(GDRIVE_CONN).extra)
try:
     gdrive_cred = json.loads(gdrive_cred["keyfile_dict"])
except:
    gdrive_cred = json.loads(gdrive_cred["extra__google_cloud_platform__keyfile_dict"])
s3_access_key = BaseHook.get_connection(S3_CONN_ID).login
s3_access_secret = BaseHook.get_connection(S3_CONN_ID).password

configs = {
    'Nigeria': {
     'files_list_spreadsheet_id' :'1ZjsRW9ggyGhTlAIdMZcjb1gxpZSN9DRS90E8kw56CV0' , 
     'file_type_to_consider' : ['csv','xlsx','xls','xlsb'],
     'text_to_exclude_from_file_path':['archive','ignore','not in use','balance','closing bal','statement_enquiry'],
     'accounts_to_exclude' : ['1456294137'], 
     'files_to_exclude' : ['20220101-20220105 1456294144.xlsx','2031624521_0848419.xlsx','2031624521_5535386.xlsx','2031687256_0443949.xlsx','2032716827_1406801.xlsx','2041327317_4616949.xlsx','5100267688.xlsx','1300010784.xlsx','Report_1018269366_200497253.xls','Report_1140210830_-517119478.xls']
    },
     #'Ghana' :{'files_list_spreadsheet_id' :''}
     }

DEBUG = False


def download_file_from_gdrive(file_details):
    scope = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_info(gdrive_cred)
    creds = credentials.with_scopes(scope)
    drive_service = build('drive', 'v3', credentials=creds)

    file_id = file_details['Id']
    #file_name = file_details['Name']
    file_path = file_details['Path']
    # Request metadata of the file
    # file = drive_service.files().get(fileId=file_id).execute()

    # Create a file object for downloading
    request = drive_service.files().get_media(fileId=file_id)

    # Create a writable file stream for downloading
    local_path =  file_path
    folder_path = os.path.dirname(local_path)
    os.makedirs(folder_path) if not os.path.exists(folder_path) else None
    fh = io.FileIO(local_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    

    # Download the file
    done = False
    while done is False:
        status, done = downloader.next_chunk()

def get_current_list_of_files_gdrive_from_file_snapshot(country):
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    credentials = service_account.Credentials.from_service_account_info(gdrive_cred,scopes=scope)
    sheets_service = build('sheets', 'v4', credentials=credentials)
    sheet_id = configs[country]['files_list_spreadsheet_id']
    file_type_to_consider =  '|'.join( configs[country]['file_type_to_consider'] )
    text_to_exclude_from_file_path = '|'.join( configs[country]['text_to_exclude_from_file_path'] )


    # Open the Folders sheet
    files_list_sheet_id = sheet_id
    sheets_service = build('sheets', 'v4', credentials=credentials)

    # Get list of sheets in the file
    spreadsheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=files_list_sheet_id).execute()
    sheets = spreadsheet_metadata.get('sheets', [])

    all_data = []

    for sheet in sheets:
        sheet_title = sheet['properties']['title']

        # Exclude Sheet1
        if sheet_title == 'Sheet1':
            continue

        # Read data from the sheet
        sheet_range = f"{sheet_title}!A:F"
        sheet_data = sheets_service.spreadsheets().values().get(spreadsheetId=files_list_sheet_id, range=sheet_range).execute()
        sheet_values = sheet_data.get('values', [])
        if sheet_values[-1][0] != 'End of List!':
            return pd.DataFrame() #Return empty df if any of teh sheets doesn't have End of List! test
        
        # Append data to the all_data list
        all_data.extend(sheet_values[1:])

    # Create a DataFrame from all_data
    columns = sheet_values[0] if all_data else None
    files_df = pd.DataFrame(all_data, columns=columns)
    #remove 'End of List!' lines
    files_df = files_df[files_df['Full Path'] != 'End of List!']

    # Add 'tmp/Bank Statements/' prefix to the 'Full Path' column
    files_df['Full Path'] = '/tmp/'+country+'/Bank Statements/' + files_df['Full Path']  #Add additional path
    files_df = files_df[files_df['Type'].str.match(file_type_to_consider, case=False)] #Take required extensions only, exact match
    files_df = files_df[~files_df['Full Path'].str.contains(text_to_exclude_from_file_path,case=False)] #Where path does not have archive or ignore or not in use
    files_df = files_df[files_df['Full Path'].str.split('/').str[-3] != 'Bank Statements'] #Ignore files which are very next to bank name folder eg: Bank Statements/GTB/0227454122.xlsx
    
    return  files_df


def get_already_processed_list_of_files(country):
    con=redshift_connector.connect(database= 'paystackharmonyredshift', host='paystackharmony-cluster.csptbdy4xa4g.eu-west-1.redshift.amazonaws.com', 	
    ############
    #Below should be changed
    user= rs_username, password= rs_password)
    cur = con.cursor()


    sql =f"SELECT filepath_original, deleted from gdrive_ingestions.bankstatements_files where filepath_original like '/tmp/%{country}%2023%'"

    #cur.execute(sql)
    #result: tuple = cur.fetchall()
    #print(result)
    #required_rs_list = [item for sublist in result for item in sublist]
    rs_df = pd.read_sql(sql,con)
    return rs_df


def get_rename_columns():
    con=redshift_connector.connect(database= 'paystackharmonyredshift', host='paystackharmony-cluster.csptbdy4xa4g.eu-west-1.redshift.amazonaws.com', 	
    ############
    #Below should be changed
    user= rs_username, password= rs_password)
    cur = con.cursor()


    sql =f"select from_name,to_name from gdrive_ingestions.rename_columns"
    rs_df = pd.read_sql(sql,con)
    # Convert DataFrame to a dictionary mapping old names to new names
    rename_columns = dict(zip(rs_df['from_name'], rs_df['to_name']))
    return rename_columns


def delete_data_redshift(to_delete):
    con=redshift_connector.connect(database= 'paystackharmonyredshift', host='paystackharmony-cluster.csptbdy4xa4g.eu-west-1.redshift.amazonaws.com', 	
    ############
    #Below should be changed
    user= rs_username, password= rs_password)
    cur = con.cursor()
    to_delete_file_name_list = [os.path.basename(path) for path in to_delete]

    sql ="delete from gdrive_ingestions.bank_transactions where filename in ('" + "', '".join(to_delete_file_name_list) + "');"
    print(sql)
    res = cur.execute(sql)
  

    # sql ="delete from gdrive_ingestions.bank_transactions_tagged where filename in ('" + "', '".join(to_delete_file_name_list) + "');"
    # print(sql)
    # cur.execute(sql)


    sql ="update gdrive_ingestions.bankstatements_files set deleted = 1 where filepath_original in ('" + "', '".join(to_delete) + "');"
    print(sql)
    cur.execute(sql)

    con.commit()
    

def test_excel(filename):
	try:
		open_workbook(filename)	
	except XLRDError:
		return False
	#except Exception as e: #ndv 20220803 adding this as it's not excel if it fails
	#	return False	
	else:
		return True


def decrypt(filepath,password_in):
	decrypted = io.BytesIO()
	with open(filepath, "rb") as f:
		file = msoffcrypto.OfficeFile(f)
		file.load_key(password=password_in)  # Use password
		file.decrypt(decrypted)
	return decrypted

def erase(file_name, searches,searchesbottom):
	"""
	This function will delete all line from the givin top
	until it finds two specified column names (headers)
	"""
	try: 
		with open(file_name, 'r+') as fr: 
			lines = fr.readlines()
		with open(file_name, 'w+') as fw:
			delete = True
			#deletebottom = False
			for line in lines:
				#if column1 in line and column2 in line:
				if delete:
					for search in searches:
						if search[0].upper() in line.upper()  and search[1].upper()  in line.upper() and len(line)<10000:
							delete = False
							print("stop deleting:",line)
				else:
					for searchbottom in searchesbottom:
						if (searchbottom[0].upper() in line.upper()  and searchbottom[1].upper()  in line.upper()):
							delete = True
				if not delete:
					fw.write(line)
	except RuntimeError as ex: 
		print(f"erase error:\n\t{ex}")
		logger.warning(f"erase error:\n\t{ex}")

def PrintException():
	exc_type, exc_obj, tb = sys.exc_info()
	f = tb.tb_frame
	lineno = tb.tb_lineno
	filename = f.f_code.co_filename
	linecache.checkcache(filename)
	line = linecache.getline(filename, lineno, f.f_globals)
	print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
	logger.warning('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))




def statements_monthly_nigeria_ingest(params,**kwargs):
    delete_threshhold = params['delete_threshhold']
    print(f'Delete Threshhold is set to {delete_threshhold} files')
    try:
         
        logger.info('Ingestion Started')
        for key,rec in configs.items():
            country_name = key
            accounts_to_exclude = rec['accounts_to_exclude']
            files_to_exclude = rec['files_to_exclude']

            rs_df = get_already_processed_list_of_files(country_name)

            gd_df = get_current_list_of_files_gdrive_from_file_snapshot(country_name) #return type is df
            
            # Perform left join on key 'filepath_original' = 'Full Path' where 'deleted' = 0
            to_delete = rs_df[rs_df['deleted'] == 0].merge(gd_df, left_on='filepath_original', right_on='Full Path', how='left')

            # Filter rows where 'Full Path' is NULL (NaN)
            to_delete = to_delete[to_delete['Full Path'].isnull()]
            #print(to_delete['filepath_original'].to_list())

            to_load = gd_df.merge(rs_df, left_on='Full Path', right_on='filepath_original', how='left')
            to_load = to_load[to_load['filepath_original'].isnull()]
            #Exclude account numbers
            # Step 1: Filter based on 'File Path' column and exclusions_1
            to_load = to_load[~to_load['Full Path'].str.contains('|'.join(accounts_to_exclude))]

            # Step 2: Further filter based on 'Name' column and exclusion_2
            to_load = to_load[~to_load['Name'].isin(files_to_exclude)]

            to_delete_list = to_delete['filepath_original'].to_list()
            
            logger.info(f'List of Files to be Deleted : {to_delete_list}')
            logger.info(f"List of Files to be Loaded : {to_load['Full Path'].to_list()}")
            

            #Delete data from redshift
            if len(to_delete_list) > delete_threshhold:
                message = f'Warning : Unusual number of files to be deleted {len(to_delete_list)}, Exiting the script'
                logger.critical(message)
                sys.exit(1)
            if len(to_delete_list) > 0:
                delete_data_redshift(to_delete=to_delete_list)   

            fileslist = []
            for index, row in to_load.iterrows():
                print(row)
                is_managed = 1 if "Managed Account" in row['Full Path'] else 0
                if "Managed Account" in row['Full Path']:
                    bank = row['Full Path'].split('/')[-4]
                    accountnumber = row['Full Path'].split('/')[-1][0:10]
                else:
                    bank = row['Full Path'].split('/')[-3]
                    folder_name = row['Full Path'].split('/')[-2]
                    if ' ' in folder_name:
                        accountnumber = folder_name.split(' ')[0]
                    else:
                        accountnumber = folder_name[0:10]
                print(bank,accountnumber)    
                fileslist.append([bank,accountnumber,row['Full Path'],is_managed,row['Id']])
                
                

            filesdf = pd.DataFrame(fileslist,columns=['Bank','Accountnumber','Path','IsManagedAccount','Id'])
            #filesdf.to_csv("/tmp/accounts.csv")
            #sys.exit(1)



            if 1==1:
                con=psycopg2.connect(dbname= 'paystackharmonyredshift', host='paystackharmony-cluster.csptbdy4xa4g.eu-west-1.redshift.amazonaws.com', 
                
                #port= '5439', user= 'niel_dv', password= secrets.secrets.redshift_password)
                #conn = S3Connection('AKIAXAYWUXO2RZTD3THK',secrets.secrets.aws_secretkey)
                port= '5439', user= rs_username, password= rs_password)
                conn = S3Connection(s3_access_key,s3_access_secret)
                
                cur = con.cursor()
                b = Bucket(conn, 'paystack-datalake')
                k = Key(b)
                s3location = 'paystackharmony/Adhoc/settlement_reports/'
                

            #exclusions =['1014801258','1014891000']
            # exclusions =[
            # 	#'2031624521'#FBN RUBBISH
            # 	#,'0604724567' #JUST FOR NOW, FILES ARE GIVING ISSUES
            # 	'1456294137' #access account with issues
            # ] 

            # fileexclusions = ['20221117 - 20221120 1456294120.xls',
            # '20221107 - 20221120 1518687060.xls',
            # '20221101-20221130 0604724529 GTB 737 Account v2.xlsx',
            # '20221129 1456294144.xls', #empty,
            # '20230126-20230129 1456294144.xls' #balances file
            # ,'20230101-20230131 3000004018 Kuda Bank Statement.xlsx' #unusable format
            # #,'20230101-20230131 1100621302 Kuda Bank Statement.xlsx' #unusable format
            # ,'20230315 1456294120.csv' #no delimiter
            # ]



            truncate = True
            merge = True
            process = True
            combine = True
            tableau = False
            tag = True
            print('Line 385')
            sys.argv = sys.argv[8:] #This is specific to Airflow so intiial two sys args are removed for the purpose of this script
            print(sys.argv)
            if 'parinitial' in sys.argv:
                truncate = True
                merge = False
                combine = False
                tableau = False

            if 'par' in sys.argv:
                truncate = False
                merge = False
                combine = False
                tableau = False

            if 'parfinal' in sys.argv:
                truncate = False
                process = False

            if 'combine' in sys.argv:
                truncate = False
                process = False
                merge = False

            if 'tableau' in sys.argv:
                truncate = False
                process = False
                merge = False
                combine = False

            if 'dontcombine' in sys.argv:
                combine = False
                tableau = False

            if 'donttag' in sys.argv:
                combine = False
                tableau = False
                tag = False


            logger.info(f"truncate {str(truncate)} process {str(process)} merge {str(merge)}")

            if truncate:
                sql ="TRUNCATE TABLE gdrive_ingestions.bank_wrk_transactions_import;"
                cur.execute(sql)
                con.commit()
                

            emptyfileslist = []


            if process:
                if 1==0:
                    logger.info("getting files with no transactions")
                    sql ="select filepath_original from gdrive_ingestions.bankstatements_files bf \
                    left join gdrive_ingestions.bank_transactions bt on \
                    right(bf.filepath_original,len(bt.filename)) = bt.filename and bf.accountnumber = bt.accountnumber \
                    where bt.filename is null \
                    and bf.filepath_original like '%202210%' and bf.deleted = 0 and filesizebytes >= 4000 and bf.filepath_original not like '%select%' and bf.filepath_original not like '%20221101 - 20221102 1414069676%'"
                    cur.execute(sql)
                    emptyfiles: tuple = cur.fetchall()
                    emptyfileslist = [a_tuple[0] for a_tuple in emptyfiles]

                for index,row in filesdf.iterrows():
                    #print(row)
                    #filelocationid = row['filelocationid'] 
                    accountnumber = row['Accountnumber']
                    # if accountnumber in exclusions:
                    # 	continue
                    bankname = row['Bank']
                    file = row['Path']

                    is_managed = row['IsManagedAccount']
                    # if "Managed Account" in file:	
                    # 	is_managed = 1
                    filename = file.split('/')[-1]
                    
                         
                    # if filename in fileexclusions:
                    # 	continue
                   
                    if len(sys.argv)>1:          
                        if sys.argv[1].isdecimal():
                            logger.info("searching for decimal")
                            if sys.argv[1] not in file:
                                continue
                        elif "," in sys.argv[1]:
                            #print("searching through list")
                            go = False
                            accs = sys.argv[1].split(",")
                            for acc in accs:
                                if acc in file:
                                    go = True
                                    logger.info(f"match in list str(file)")
                            if not go:
                                continue

                        else:
                            if bankname != sys.argv[1]   and 'par' not in sys.argv[1] and filename != sys.argv[1] and 'dontcombine' not in sys.argv[1] and filename not in sys.argv[1] and 'donttag' not in sys.argv[1]:
                                   
                                continue
                    
                    #filelocation = row['filelocation']
                    #cur.execute('CALL reconciliations.log_file_get_type (%s,%s);',
                    #print("checking account",accountnumber)


                    xxx, fileextension = os.path.splitext(file)
                    #filesize =str(os.path.getsize(file)) 
                    
                    #filesize =os.path.getsize(file)

                
                    if 1==1:
                    
                        if len(sys.argv)>2:
                            #print(sys.argv[2])
                            if sys.argv[2] not in file and 'par' not in sys.argv[2]:
                                continue
                        #print(file)

                        
                        #print("file extension",fileextension)
                        #fileid = cur.execute("CALL reconciliations.log_file(%s,%s,%s);",(file,newfilename,filetypeid))
                        #fileid = cur.execute("CALL reconciliations.log_file(%s,%s,%s);",(file,newfilename,filetypeid))
                        
                        #try:
                        if 1 == 1:
                        
                            if fileextension !="" and "ignore" not in file and ".sb-7e8547e5-nW1uKD" not in file and "April 2022 POS Managed accounts.xlsx" not in file and "closing bal" not in file and "balance" not in file.lower() and "statement_enquiry" not in file.lower() and "~$" not in file:
                                #print("filesize",filesize,"extension",fileextension,"accountnumber",accountnumber,"bankname",bankname)
                                #cur.execute("CALL gdrive_ingestions.load_bankstatements_files(%s,%s,%s,%s,%s,%s);",(file,bankname,is_managed,accountnumber,fileextension,filesize ))
                                #con.commit()
                                logger.info(row)
                                
                                download_file_from_gdrive(row)
                                
                                newfilename = file.replace('/tmp','/tmp/historic')
                                newfilename = newfilename.replace(fileextension,'.csv')
                                newfilename_xlsx = file.replace('/tmp','/tmp/historic')
                                newfilename_xlsx = newfilename_xlsx.replace(fileextension,'_xlsx.xlsx')
                                readyfilename = newfilename.replace("csv","_ready.csv")
                                #print("readyfilename",readyfilename)
                                
                                
                                #if (not os.path.isfile(readyfilename) and not os.path.isfile(readyfilename.replace(' (1)',''))) or (len(sys.argv) > 2) or (file in emptyfileslist):# or 1==1:
                                #Above condition is modified so if file exists in historic folder it will still process it
                                if 1==1:
                                     
                                    #if (not os.path.isfile(newfilename)) or (len(sys.argv) > 2 and 'par' not in sys.argv[2] and (',' not in sys.argv[2]  ) or ','  in sys.argv[1]) :# or 1==1:

                                    filesize =os.path.getsize(file)
                                    logger.info(f"filesize {str(filesize)} extension {str(fileextension)} accountnumber {str(accountnumber)} bankname {str(bankname)}")
                                    logger.info("PROCESSING")
                                    logger.info(f"file: {str(file)}")
                                    #PRINT
                                    logger.info("start logging file")
                                    cur.execute("CALL gdrive_ingestions.load_bankstatements_files(%s,%s,%s,%s,%s,%s);",(file,bankname,is_managed,accountnumber,fileextension,filesize ))
                                    con.commit()
                                    logger.info("finished logging file")
                                    if not os.path.exists(os.path.dirname(newfilename)):
                                        os.makedirs(os.path.dirname(newfilename))

                                    #if password != None:
                                    #	decrypted = decrypt(os.path.join(file),password)
                                    #	read_file = pd.read_excel(decrypted)
                                    #	read_file.to_csv (newfilename, index = None, header=True)
                                    
                                    #ishtml =bool(BeautifulSoup(html, "html.parser").find())

                                    if filesize <= 67:
                                        logger.error("file corrupted")
                                    else:

                                        ishtml = False
                                        istabbed = False
                                        tryexcel = False				#20230304 1389145393.csv
                                        if not test_excel(file) and ".xlsx" not in file:
                                            logger.info("not excel")
                                                
                                            try:
                                                with open(file) as f:
                                                    lines = f.readlines()	
                                            except Exception as e:
                                                logger.error(str(e))
                                                logger.error("Can't open file as text to test. Trying different encoding")
                                                with open(file,encoding="ISO-8859-1") as f:
                                                    lines = f.readlines()	
                                                    
                                                #print("lines0",lines[0])
                                                #print("lines1",lines[1])
                                                #print("lines2",lines[2])
                                                #if bool(BeautifulSoup(lines[0], "html.parser").find())\
                                                #	or bool(BeautifulSoup(lines[1], "html.parser").find())\
                                                #	or bool(BeautifulSoup(lines[2], "html.parser").find()):
                                            
                                            try:
                                                if "html" in lines[0] or "html" in lines[1] or "html" in lines[2] or ("<" in lines[0] and ">" in lines[0]):
                                                    ishtml = True
                                                elif len(lines)>10:
                                                    if "html" in lines[12]:
                                                        ishtml = True
                                                if	(" " in lines[0] and '"' in lines[0] and ',' not in lines[0]) or ("\t" in lines[0]):# and not ('‡°±' in lines[0]): # THIS SECOND PART IS EXCLUDING XLS FILES THAT ARE ACTUAL EXCEL FILES
                                                    istabbed = True
                                                logger.info(f"Is this html? {str(ishtml)}")
                                                logger.info(f"Is this tabbed? {str(istabbed)}")
                                            except Exception as e:
                                                
                                                logger.info("Testing for html failed")
                                                logger.info(str(e))

                                        else:				#20230307 NDV ADDING THIS SINCE ACCESS GIVES XLS FILES WITH CSV EXTENSIONS
                                            print("Might be excel")
                                            tryexcel = True		

                                            

                                        if 1==2:
                                            print("adsf")
                                        else:
                                            if ishtml:
                                                logger.info("trying first read - METHOD 1")
                                                try:
                                                    #print("trying first read - METHOD 1")

                                                    #METHOD 1
                                                    #with xw.App(visible=False) as app:
                                                    
                                                    #tmpfile = '~/Library/Containers/com.microsoft.Excel/Data/file.txt'
                                                    #shutil.copyfile(file, tmpfile)

                                                    #if '.xlsx' not in file and '.xls' in file:
                                                    
                                                    tmpfile = '/tmp/file.xls'
                                                    shutil.copyfile(file, tmpfile)
                                                    #newfilename = file.replace('.xls','.xlsx')
                                                    #newfilename = 'converted.xlsx'

                                                    #read_xls(file,newfilename)
                                                    #read_xls(tmpfile,newfilename)									#
                                                    if os.path.exists(newfilename_xlsx):
                                                        os.remove(newfilename_xlsx)
                                                    with xw.App() as app:
                                                        #wb = xw.Book(file)
                                                        app.display_alerts = False
                                                        #wb = app.books.open(tmpfile)
                                                        #wb = app.books.open(file)
                                                        wb = app.books.open(tmpfile) 
                                                        wb.save(newfilename_xlsx)
                                                        wb.close()
                                                    table = pd.read_excel(newfilename_xlsx)
                                                    

                                                except Exception as e:
                                                    logger.info(str(e))
                                                    logger.info("trying first read - METHOD 2")
                                                    try:
                                                        # METHOD 2

                                                        table = pd.read_html(file,match='Credit',header=0)[0]
                                                        #print(table)
                                                        if len(table.columns[0]) >1000:
                                                            logger.info("long column, trying alternative")
                                                            table = pd.read_html(file,match='Credit',flavor = 'html5lib',header=0)[0]
                                                            #print(table)
                                                            #print(1/0) #make it fail
                                                        #print(table)
                                                        #table = pd.read_html(file,match='Credit')[1]
                                                        #if len(table.columns[0]) >1000:
                                                        #	print(1/0)	#make it fail if it puts everything in one line
                                                    except Exception as e:
                                                    
                                                    #METHOD 3
                                                        logger.info(str(e))

                                                        logger.info("trying first read - METHOD 3")
                                                        try:

                                                            parser = ExcelParser(file)
                                                            parser.to_excel(newfilename_xlsx)
                                                            table = pd.read_excel(newfilename_xlsx)
                                                            #table = pd.read_html(file,match='Credit',header=0)[0]
                                                            #table = pd.read_html(file,match='Credit')[1]


                                                        #except Exception as e:
                    


                                                        #METHOD 3	
                                                        #workbook = gateway.jvm.ExcelDocument()
                                                        #if workbook.easy_LoadHTMLFile(file):
                                                        #	#workbook.easy_getSheetAt(0).setSheetName("First tab")
                                                        #	workbook.easy_WriteXLSXFile(newfilename_xlsx)
                                                        #	table = pd.read_excel(newfilename_xlsx)


                    

                                                    #app = xw.App()
                                                    #wb = xw.Book(file)
                                                    #wb = app.books.open(file)
                                                    #wb = Workbook(file)
                                                    
                                                    


                                                        #print("finished first read")
                                                        #print("first read columns",table.columns)
                                                        except Exception as e:
                                                            logger.info("first read failed")
                                                            logger.info(str(e))
                                                            print(1/0)
                                                    #table = pd.read_html(file,match='Description',header=0)[0]
                                                logger.info("finished first read")
                                                #print("first read columns",table.columns)
                                                #print(table)
                                                table = table.replace(r'\r', '', regex=True)

                                                table.to_csv(newfilename, index = None, header=True)
                                            elif fileextension == '.xlsb':
                                                read_file = pd.read_excel(file, engine='pyxlsb')
                                                try:
                                                    read_file = read_file.str.replace(r'\r', '', regex=True)
                                                except Exception as e:
                                                    logger.info("replace doesn't work")

                                                read_file.to_csv (newfilename, index = None, header=True)
                                            elif istabbed:
                                                table = pd.read_csv(file,sep = "\t")
                                                try:
                                                    table = table.replace(r'\r', '', regex=True)
                                                except Exception as e:
                                                    logger.info("replace doesn't work 2")

                                                table.to_csv(newfilename, index = None, header=True)
                                            elif fileextension == '.csv' and not tryexcel:
                                                logger.info("this is csv")
                                                if bankname =='Sterling':
                                                    #table = pd.read_csv(file,sep="|")
                                                    table = pd.read_csv(file,sep="{")
                                                    headers = table.columns.str.split('|')[0]
                                                    logger.info(str(headers))
                                                    table['pipes'] = table.iloc[:, 0].str.count('\\|')
                                                    if max(table['pipes']) == 21:
                                                        logger.info("replacing")
                                                        table = table.replace('TO STERLING BANK\|','TO STERLING BANK ', regex=True)
                                                        table = table.replace('TO STERLING\|','TO STERLING ', regex=True)
                                                    table = table.iloc[:, 0].str.split('|',expand = True)
                                                    table.columns = headers
                                                    #print(table.columns)

                                                    #table.to_csv('table.csv')
                                                    #TO STERLING BANK|
                                                    #print(table.columns)
                                                    #print(table)
                                                    #print(table['pipes'])
                                                    #sys.exit()
                                                else:
                                                    if bankname == 'Titan Trust': #and is_managed ==1:	# NDV 20221011 take away is managed

                                                        table = pd.read_csv(file, skiprows=1, header=None,usecols=[0,1,2,3,4,5,6,7,8])
                                                        table.columns =['txN_DATE','coD_USER_ID','coD_CC_BRN_TXN','description','reference','valuE_DATE','deposit','withdrawal','balance']

                                                    else:
                                                        table = pd.read_csv(file)#, engine='python'
                                                logger.info(str(table))
                                                table = table.replace(r'\r', '', regex=True)

                                                table.to_csv(newfilename, index = None, header=True)							
                                            else:
                                                try:
                                                    logger.info("try read excel")
                                                    read_file_list = pd.read_excel(file,sheet_name = None)
                                                    read_file = pd.concat(read_file_list)
                                                except Exception as e:
                                                    password = accountnumber[3:8]
                                                    #print("trying password",password)
                                                    decrypted = decrypt(os.path.join(file),password)
                                                    
                                                    read_file = pd.read_excel(decrypted)

                                                read_file = read_file.replace(r'\r', '', regex=True)
                                                
                                                read_file.to_csv (newfilename, index = None, header=True)		
                                        #print("erase: match column:", match_column,"date_column:",date_column)

                                        #print("r"read_file)
                                        
                                        logger.info("finished reading table")

                                        erasesearches = [['date','credit'],['TRAN_DATE','CR_AMT'],['Txn Date','Balance'],['Transaction Date','Description'],
                                            ['Transaction Type','Transaction Amount'],['narration','balance'],['Trans Date','Remarks'],
                                            ['Transaction Date','Deposit'],['TRN_DT','TXN_AMT'],['Date','Deposit'],
                                            ['txN_DATE','toT_WITHDRAWAL'],['Transaction Date','Transaction Description'],['Transaction Date','Transaction Details']
                                            ,['CreatedDate','Description1'],[' Balance',' Credit Amount'],['TRN_DT','TXN AMT'],['TRAN_DATE','NARRATION'],
                                            ['TRANSACTION_DETAILS','TRANSACTION_DATE'],['Create Date','Description'],['Value Date','Narration'],['Date','Description']
                                            ,['TRA DATE','REMARKS'],['VALUE_DT','TXN_AMT'],['TRAN_DATE','Money In'],['Transaction_type','tran_posted_datetime'],
                                            ['TRN_DT','LCY_AMOUNT'],['tra_date','remarks'],['Account Description : ','*PAYSTACK SOUTH AFRICA (PTY) L'],['datetimelocal','total_impact'],
								['Post Date','Transaction Details']
                                        ]

                                        erasesearches_bottom = [['Total Debits','Total Credits:'],['TOTAL DEBIT',''],['Please report any discrepancies in this statement within 15 days of receipt. Failure to do so implies the statement is correct','']
							   ,['****End of Account Details****','']]
                                        logger.info("start erasing")
                                        erase(newfilename, erasesearches,erasesearches_bottom)

                                        #print("start erasing bottom")
                                        #erase(newfilename, erasesearches)

                                        logger.info("finished erasing")
                                    #
                                    # 
                                    #if 1==1: #NDV 20220302 UNCOMMENTING ONE BACK TO MAKE SURE WE ALWAYS REDO READY FILE. REMOVE THE # TO PROCESS READY FILE. ADD IT BACK IN TO NOT.
                                        #erase(newfilename,"date","credit")
                                        
                                        alldata = pd.read_csv(newfilename)

                                        logger.info("finished reading from csv")
                                        
                                        # renamecolumns_list = get_rename_columns()
                                        # print('List of columns to be renamed')
                                        # print(renamecolumns_list)

                                        renamecolumns = {
                                        'Create Date': 'TRAN_DATE',
                                        'DATE POSTED': 'TRAN_DATE',
                                        'TRA_DATE': 'TRAN_DATE',
                                        'TRA DATE': 'TRAN_DATE',
                                        'Txn Date': 'TRAN_DATE',
                                        'Transaction Date': 'TRAN_DATE',
                                        'Tran Date': 'TRAN_DATE',
                                        'TRN_DT': 'TRAN_DATE',
                                        'Date': 'TRAN_DATE',
                                        'txN_DATE': 'TRAN_DATE',
                                        'Trans Date': 'TRAN_DATE',
                                        'CreatedDate': 'TRAN_DATE',
                                        'TRANSACTION_DATE': 'TRAN_DATE',
                                        'BOOKING.DATE': 'TRAN_DATE',
                                        'TRANSACTION DATE': 'TRAN_DATE',
                                        'TXN DATE': 'TRAN_DATE',
                                        'Post Date':'TRAN_DATE',
                                        'tran_date':'TRAN_DATE',
                                        'DATE':'TRAN_DATE',
                                        'Creation Date':'TRAN_DATE',
                                        'transaction_date':'TRAN_DATE',
                                        'TRANSACTIONDATE':'TRAN_DATE',
                                        'tran_posted_datetime':'TRAN_DATE',
                                        #'datetime_req':'TRAN_DATE',
                                        'STMT_DAT':'TRAN_DATE',
                                        'tra_date': 'TRAN_DATE',
                                        'datetimelocal':'TRAN_DATE',

                        
                                        
                                        'Value Date': 'VALUE_DATE',
                                        'Effective Date': 'VALUE_DATE',
                                        ' Effective Date': 'VALUE_DATE',
                                        'VALUEDATE': 'VALUE_DATE',
                                        'valuE_DATE': 'VALUE_DATE',
                                        'VAL DATE': 'VALUE_DATE',
                                        'VALUE DATE': 'VALUE_DATE',
                                        'VALUE_DT':'VALUE_DATE',
                                        'value_date':'VALUE_DATE',
                                        'val_date':'VALUE_DATE',


                                        'DESCRIPTION': 'MATCH_COLUMN',
                                        'REMARKS': 'MATCH_COLUMN',
                                        'TRAN_PARTICULAR': 'MATCH_COLUMN',
                                        'NARRATIVE': 'MATCH_COLUMN',
                                        'Description': 'MATCH_COLUMN',
                                        'Transaction Description': 'MATCH_COLUMN',
                                        'Narration': 'MATCH_COLUMN',
                                        'narration': 'MATCH_COLUMN',
                                        'Transaction Remarks:': 'MATCH_COLUMN',
                                        'description': 'MATCH_COLUMN',
                                        'Remark': 'MATCH_COLUMN',
                                        'Transaction Details': 'MATCH_COLUMN',
                                        'Remarks': 'MATCH_COLUMN',
                                        'Description1': 'MATCH_COLUMN',
                                        ' Description/Payee/Memo': 'MATCH_COLUMN',
                                        'Description/Payee/Memo': 'MATCH_COLUMN',
                                        'TRANNARRATION': 'MATCH_COLUMN',
                                        'Transaction_type': 'MATCH_COLUMN',	#ZENITH 8931963971
                                        'tran_type_description':'MATCH_COLUMN',

                                        'NARRATION': 'MATCH_COLUMN',
                                        'TRANSACTION_DETAILS': 'MATCH_COLUMN',
                                        'Description/ Payee/Memo': 'MATCH_COLUMN',
                                        'remarks': 'MATCH_COLUMN',
                                        'Transaction Particulars': 'MATCH_COLUMN',

                                        

                                        
                                        'DEBIT': 'DEBIT_AMOUNT',
                                        'DR_AMT': 'DEBIT_AMOUNT',
                                        'Debit': 'DEBIT_AMOUNT',
                                        'Withdrawals': 'DEBIT_AMOUNT',
                                        'Withdrawal': 'DEBIT_AMOUNT',
                                        'Money Out': 'DEBIT_AMOUNT',
                                        'Debit Amount': 'DEBIT_AMOUNT',
                                        'withdrawal': 'DEBIT_AMOUNT',
                                        'Dedit Amount': 'DEBIT_AMOUNT',
                                        ' Debit Amount': 'DEBIT_AMOUNT',
                                        'WITHDRAWAL': 'DEBIT_AMOUNT',
                                        'DEBIT AMT': 'DEBIT_AMOUNT',
                                        'Paid Out': 'DEBIT_AMOUNT',
                                        'Money_out': 'DEBIT_AMOUNT',
                                        'debit': 'DEBIT_AMOUNT',
                                        'DR': 'DEBIT_AMOUNT',	
                                        'Debit(GHS)':'DEBIT_AMOUNT',			
                                        

                                        'CREDIT': 'CREDIT_AMOUNT',
                                        'CR_AMT': 'CREDIT_AMOUNT',
                                        'Credit': 'CREDIT_AMOUNT',
                                        'Lodgments': 'CREDIT_AMOUNT',
                                        'Deposit': 'CREDIT_AMOUNT',
                                        'Money In': 'CREDIT_AMOUNT',
                                        'Credit Amount': 'CREDIT_AMOUNT',
                                        'deposit': 'CREDIT_AMOUNT',
                                        'Lodgment': 'CREDIT_AMOUNT',
                                        ' Credit Amount': 'CREDIT_AMOUNT',
                                        'LODGEMENT': 'CREDIT_AMOUNT',
                                        'CREDIT AMT': 'CREDIT_AMOUNT',
                                        ' LODGEMENT ': 'CREDIT_AMOUNT',
                                        'Paid In': 'CREDIT_AMOUNT',
                                        'Money_in': 'CREDIT_AMOUNT',
                                        'credit': 'CREDIT_AMOUNT',
                                        'CR': 'CREDIT_AMOUNT',
                                        'Credit(GHS)':'CREDIT_AMOUNT',							


                                        'CRNT BAL': 'BALANCE_AMOUNT',
                                        'BALANCE': 'BALANCE_AMOUNT',
                                        'Balance': 'BALANCE_AMOUNT',
                                        'Closing Balance': 'BALANCE_AMOUNT',
                                        'TRANBALANCE': 'BALANCE_AMOUNT',
                                        'Account Balance': 'BALANCE_AMOUNT',
                                        #'closinG_BAL': 'BALANCE_AMOUNT',
                                        'balance': 'BALANCE_AMOUNT',
                                        ' Balance': 'BALANCE_AMOUNT',
                                        'RELATED_ACCOUNT': 'BALANCE_AMOUNT',		#ACCESS 2318 JUNK FILES APRIL 2022
                                        'BALANCE': 'BALANCE_AMOUNT',
                                        'CRNT_BAL': 'BALANCE_AMOUNT',
                                        'RUNNING_SUM': 'BALANCE_AMOUNT',
                                        'RUNNING_SUM ': 'BALANCE_AMOUNT',
                                        'RunningBalance': 'BALANCE_AMOUNT',
                                        ' RUNNING_SUM ': 'BALANCE_AMOUNT',
                                        'RUNNING BALANCE': 'BALANCE_AMOUNT',
                                        ' RUNNING BALANCE ': 'BALANCE_AMOUNT',
                                        'balance': 'BALANCE_AMOUNT',
                                        'crnt_bal': 'BALANCE_AMOUNT',
                                        'RUNNINGBALANCE': 'BALANCE_AMOUNT', #ACCESS NEW FEED
                                        'Running Balance(GHS)':'BALANCE_AMOUNT',




                                        'AMOUNT':'TXN_AMT',
                                        'Amount':'TXN_AMT',
                                        'tran_amount':'TXN_AMT',
                                        'LCY_AMOUNT':'TXN_AMT',
                                        #'total_impact':'TXN_AMT',


                                        'DEBITCREDIT':'DRCR',
                                        'Posting Type':'DRCR',
                                        'DRCR_IND':'DRCR',


                                        
                                        'REF_NO':'TRAN_ID',
                                        'Ref No.':'TRAN_ID',
                                        'EXTERNAL_REF_NO':'TRAN_ID',
                                        'reference':'TRAN_ID',
                                        'TRN_REF_NO':'TRAN_ID',
                                        'REFNO':'TRAN_ID',
                                        'Instrument Number':'TRAN_ID',
                                        'expl_desc':'TRAN_ID',
                                        'UID1':'TRAN_ID',
                                        'EXPL DESC':'TRAN_ID',
                                        'REFNO2':'TRAN_ID',
                                        'SESSION.ID':'TRAN_ID',
                                        


                                        'TRAN_RMKS':'TRAN_ID',
                                        'Reference / Cheque Number':'TRAN_ID',
                                        'card_acceptor_name_location':'TRAN_ID',
                                        'retrieval_ref_nr':'TRAN_ID',
                                        #'auth_id_rsp':'TRAN_ID'
                                        'retrieval_reference_nr':'TRAN_ID',
                                        'retrieval_ref_nr':'TRAN_ID',
                                        'card_acceptor_name_loc':'TRAN_ID'

                                        }

                    
                                        if "BALANCE" in alldata.columns and "RUNNING BALANCE" in alldata.columns:
                                            alldata.rename(columns={"BALANCE":"IGNORE THIS RUBBISH"},inplace=True)


                                        if "Balance" in alldata.columns and "RUNNING_SUM" in alldata.columns:
                                            alldata.rename(columns={"RUNNING_SUM":"IGNORE THIS RUBBISH"},inplace=True) #Access......


                                        alldata.rename(columns=renamecolumns, inplace=True)

                                        if bankname =='Nedbank SA':
                                            #alldata = alldata[['Statement Enquiry','Unnamed: 1','Unnamed: 2','Unnamed: 3','Unnamed: 4','Unnamed: 5','Unnamed: 6']]
                                            alldata = alldata.iloc[: , :5]
                                            alldata.columns=['TRAN_ID','TRAN_DATE','MATCH_COLUMN','AMT.LCY','BALANCE_AMOUNT']


                    #Transaction Date,Transaction Description,Source/Destination,Money In,Money Out,Opening Balance,Closing Balance
                                        cols = []
                                        hastranid = 0
                                        for column in alldata.columns:
                                            if column =='TRAN_ID':
                                                if hastranid == 1:
                                                    cols.append('TRAN_ID_2')
                                                else:
                                                    cols.append(column)
                                                hastranid = hastranid + 1
                                            else:
                                                cols.append(column)
                                        alldata.columns = cols

                                        if hastranid == 2:
                                            alldata['TRAN_ID'] = alldata['TRAN_ID'].astype(str)+'__'+alldata['TRAN_ID_2'].astype(str)

                                        cols = []
                                        existingcolumn = 0
                                        for column in alldata.columns:
                                            if column =='TRAN_DATE':
                                                if existingcolumn == 1:
                                                    cols.append('TRAN_DATE_2')
                                                else:
                                                    cols.append(column)
                                                existingcolumn = existingcolumn + 1
                                            else:
                                                cols.append(column)
                                        alldata.columns = cols

                                        if existingcolumn == 2:
                                            alldata.drop(columns =['TRAN_DATE_2'],inplace=True)



                                        if 'TRAN_DATE' not in alldata.columns:
                                            alldata['TRAN_DATE'] = np.NaN
                                        if 'VALUE_DATE' not in alldata.columns:
                                            alldata['VALUE_DATE'] = np.NaN
                                        if 'DEBIT_AMOUNT' not in alldata.columns:
                                            alldata['DEBIT_AMOUNT'] = np.NaN
                                        if 'TRAN_AMOUNT' not in alldata.columns:
                                            alldata['TRAN_AMOUNT'] = np.NaN
                                        if 'CREDIT_AMOUNT' not in alldata.columns:
                                            alldata['CREDIT_AMOUNT'] = np.NaN
                                        if 'TRAN_ID' not in alldata.columns:
                                            alldata['TRAN_ID'] = np.NaN
                                        if 'MATCH_COLUMN' not in alldata.columns:	
                                            alldata['MATCH_COLUMN'] = np.NaN
                                        if 'BALANCE_AMOUNT' not in alldata.columns:
                                            alldata['BALANCE_AMOUNT'] = np.NaN	
                                        #print(alldata.columns)
                                        if 'Transaction Type' in alldata.columns and 'Transaction Amount' in alldata.columns:
                                            alldata.loc[alldata['Transaction Type'] == 'C', 'CREDIT_AMOUNT'] = alldata['Transaction Amount']
                                            alldata.loc[alldata['Transaction Type'] == 'D', 'DEBIT_AMOUNT'] = alldata['Transaction Amount']
                                        if 'DRCR' in alldata.columns and 'TXN_AMT' in alldata.columns:
                                            if accountnumber =='1518687060':
                                                alldata.loc[(alldata['DRCR'] == 'C') & (alldata['TXN_AMT'] < 0 ) , 'DEBIT_AMOUNT'] = alldata['TXN_AMT']
                                                alldata.loc[(alldata['DRCR'] == 'D') & (alldata['TXN_AMT'] < 0 ) , 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                                alldata.loc[(alldata['DRCR'] == 'C') & (alldata['TXN_AMT'] > 0 ) , 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                                alldata.loc[(alldata['DRCR'] == 'D') & (alldata['TXN_AMT'] > 0 ) , 'DEBIT_AMOUNT'] = alldata['TXN_AMT']	
                                            else:
                                                alldata.loc[alldata['DRCR'] == 'C', 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                                alldata.loc[alldata['DRCR'] == 'D', 'DEBIT_AMOUNT'] = alldata['TXN_AMT']

                                            alldata.loc[alldata['DRCR'] == 'credit', 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                            alldata.loc[alldata['DRCR'] == 'debit', 'DEBIT_AMOUNT'] = alldata['TXN_AMT']							
                                        if 'DRCR' in alldata.columns and 'TXN_AMT ' in alldata.columns:
                                            alldata.loc[alldata['DRCR'] == 'C', 'CREDIT_AMOUNT'] = alldata['TXN_AMT ']
                                            alldata.loc[alldata['DRCR'] == 'D', 'DEBIT_AMOUNT'] = alldata['TXN_AMT ']
                                        if 'DRCR' in alldata.columns and ' TXN_AMT ' in alldata.columns:
                                            alldata.loc[alldata['DRCR'] == 'C', 'CREDIT_AMOUNT'] = alldata[' TXN_AMT ']
                                            alldata.loc[alldata['DRCR'] == 'D', 'DEBIT_AMOUNT'] = alldata[' TXN_AMT ']
                                        if 'DRCR' in alldata.columns and 'TXN AMT' in alldata.columns:
                                            alldata.loc[alldata['DRCR'] == 'C', 'CREDIT_AMOUNT'] = alldata['TXN AMT']
                                            alldata.loc[alldata['DRCR'] == 'D', 'DEBIT_AMOUNT'] = alldata['TXN AMT']
                                        if 'AMT.LCY' in alldata.columns:
                                            alldata.loc[alldata['AMT.LCY'] > 0 , 'CREDIT_AMOUNT'] = alldata['AMT.LCY']
                                            alldata.loc[alldata['AMT.LCY'] <0, 'DEBIT_AMOUNT'] = alldata['AMT.LCY']		
                                        if 'TRANINDICATOR' in alldata.columns and 'TRANAMOUNT' in alldata.columns: #ACCESS NEW FEED
                                            alldata.loc[alldata['TRANINDICATOR'] == 'LODGEMENT', 'CREDIT_AMOUNT'] = alldata['TRANAMOUNT']
                                            alldata.loc[alldata['TRANINDICATOR'] == 'WITHDRAWAL', 'DEBIT_AMOUNT'] = alldata['TRANAMOUNT']	
                                        if 'TRANINDICATOR' in alldata.columns and 'TXN_AMT' in alldata.columns: #ACCESS NEW FEED
                                            alldata.loc[alldata['TRANINDICATOR'] == 'LODGEMENT', 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                            alldata.loc[alldata['TRANINDICATOR'] == 'WITHDRAWAL', 'DEBIT_AMOUNT'] = alldata['TXN_AMT']				
                                        if 'TXN_AMT' in alldata.columns and accountnumber == '8931963971':
                                            alldata.loc[alldata['MATCH_COLUMN'] == 'Deposit', 'CREDIT_AMOUNT'] = alldata['TXN_AMT']
                                            alldata.loc[alldata['MATCH_COLUMN'] == 'payment from account', 'DEBIT_AMOUNT'] = alldata['TXN_AMT']
                                        if 'total_impact' in alldata.columns and accountnumber == '8931963971':
                                            alldata.loc[alldata['total_impact'] > 0, 'CREDIT_AMOUNT'] = alldata['total_impact']
                                            alldata.loc[alldata['total_impact'] < 0, 'DEBIT_AMOUNT'] = alldata['total_impact']
                                            #alldata['CREDIT_AMOUNT'] = alldata['TXN_AMT']				
                                        #result = alldata[['filename','dimacquiringgatewayid','TRAN_DATE','VALUE_DATE','TRAN_AMOUNT','DEBIT_AMOUNT','CREDIT_AMOUNT','TRAN_ID','MATCH_COLUMN']]
                                        #alldata['filetypeid'] = filetypeid
                                        
                                        alldata = alldata.loc[alldata['CREDIT_AMOUNT'] !='CREDIT'] # this is for when we concatente sheets, so that the headings dont get repeated

                                        #alldata = alldata.loc[alldata['DEBIT_AMOUNT'] !='Uncleared Items'] # Old Zenith file issues
                                        
                                        
                                        #print(alldata.dtypes)

                                        logger.info(f"file {str(file)}")
                                        logger.info(f"bankname {str(bankname)}")
                                        logger.info(f"is_managed {str(is_managed)}")
                                        logger.info(f"accountnumber {str(accountnumber)}")

                                        #filesize = os.path.getsize(file) 
                                        #cur.execute("CALL gdrive_ingestions.load_bankstatements_files(%s,%s,%s,%s,%s,%s);",(file,bankname,is_managed,accountnumber,fileextension,filesize ))
                                        #con.commit()

                                        #alldata['fileid'] = fileid
                                        alldata['filename'] = filename
                                        alldata['bankname'] = bankname
                                        alldata['accountnumber'] = accountnumber

                                        # NDV 20221206 THIS IS TO CATER FOR GTB UNSORTED STATEMENTS
                                        if 'TRA_SEQ1' in alldata.columns: 
                                            logger.info("SORTING COLUMN FOUND")
                                            alldata = alldata.sort_values(['TRAN_DATE', 'TRA_SEQ1'], ascending=[1, 1])

                                        alldata['filerownumber'] = np.arange(alldata.shape[0])

                                        alldata = alldata[[ 'TRAN_DATE','VALUE_DATE','TRAN_AMOUNT','DEBIT_AMOUNT','CREDIT_AMOUNT','TRAN_ID','MATCH_COLUMN','BALANCE_AMOUNT','bankname','accountnumber','filename','filerownumber'] ]
                                        #print(alldata.dtypes)
                                        if alldata.dtypes['CREDIT_AMOUNT'] not in ['float64','int64']:
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].astype(str)
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].str.replace('nan','')
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].str.replace('(','-')
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].str.replace(')','')
                                            
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].str.replace(' -   ','')


                                        if alldata.dtypes['DEBIT_AMOUNT'] not in ['float64','int64']:
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].astype(str)
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace('nan','')
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace('(','-')
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace(')','')
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace('OPENING BALANCE','')
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace('OPENING BAL','')
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace(' -   ','')




                                        if alldata.dtypes['BALANCE_AMOUNT'] not in ['float64','int64']:
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].astype(str)
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace('nan','')
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace('(','-')
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace(')','')
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace(' -   ','')

                                            #alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace('¬†','')	# SOME RANDOM CHARACTERS IN ZENITH 1018269304
                                            #alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace('¬†','')	# SOME RANDOM CHARACTERS IN ZENITH 1018269304

                                            alldata['BALANCE_AMOUNT'].replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True) # SOME RANDOM CHARACTERS IN ZENITH 1018269304

                                        """
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace(r'\r', '', regex=True)

                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\r\n',' ')
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\r',' ')
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\n',' ', regex=True)	
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace(chr(13),' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace(chr(13),' ')


                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u000A',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u000B',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u000C',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u000D',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u0085',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u2028',' ', regex=True)
                                        alldata['MATCH_COLUMN'] = alldata['MATCH_COLUMN'].str.replace('\u2029',' ', regex=True)
                                        """

                                        #print(alldata['CREDIT_AMOUNT'])
                                        if alldata.dtypes['TRAN_AMOUNT'] not in ['float64','int64']:
                                            alldata['TRAN_AMOUNT'] = alldata['TRAN_AMOUNT'].str.replace(',','')
                                        if alldata.dtypes['DEBIT_AMOUNT'] not in ['float64','int64']:
                                            alldata['DEBIT_AMOUNT'] = alldata['DEBIT_AMOUNT'].str.replace(',','')
                                        if alldata.dtypes['CREDIT_AMOUNT'] not in ['float64','int64']:
                                            alldata['CREDIT_AMOUNT'] = alldata['CREDIT_AMOUNT'].str.replace(',','')
                                        if alldata.dtypes['BALANCE_AMOUNT'] not in ['float64','int64']:
                                            alldata['BALANCE_AMOUNT'] = alldata['BALANCE_AMOUNT'].str.replace(',','')
                                        #alldata["CREDIT_AMOUNT"] = pd.to_numeric(alldata["CREDIT_AMOUNT"])
                                        #print(alldata['CREDIT_AMOUNT'])
                                        alldata.to_csv(os.path.join(readyfilename),index=False)
                                        logger.info("finished writing to ready file")
                                    
                                    
                                    
                                        cur = con.cursor()
                            
                                        """ 
                                        fileid = cur.execute("CALL reconciliations.log_file2(%s,%s,%s,%s);",(file,bankname,is_managed,accountnumber))
                                        #cur.execute('call reconciliations.log_file_UPDATE (%s,%s);', (fileid, newfilename))
                                        con.commit()
                                        #fileid = cur.execute("CALL reconciliations.log_file('%s');",file)
                                        """

                                        #k.key = 'paystackharmony/Adhoc/settlement_reports/'+'filetoload.csv'
                                        k.key = 'gdrive_files/Adhoc/settlement_reports/'+'filetoload.csv'
                                        k.set_contents_from_filename(readyfilename)
                                        cur = con.cursor()
                                        cur.execute("CALL gdrive_ingestions.load_transactions_import();")
                                        #cur.close()
                                        con.commit()
                                        if merge:
                                            cur = con.cursor()
                                            cur.execute("truncate table gdrive_ingestions.bank_wrk_transactions_stage;")	
                                            con.commit()	
                                            logger.info("Merging transactions")	
                                            cur.execute("call gdrive_ingestions.merge_bank_transactions();")		
                                            con.commit()
                                            cur.execute("truncate table gdrive_ingestions.bank_wrk_transactions_import;")	
                                            con.commit()
                                        logger.info("finished loading data to redshift")
        logger.info('Ingestion Completed')
    except Exception as err:
        print(err)
        message = f"Function statements_monthly_nigeria_ingest failed : \n \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)




default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

default_params ={
     "delete_threshhold" : 30
}

with DAG(dag_id="statements_monthly_nigeria_ingest",
         description="statements_monthly_nigeria_ingest",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval="15 5 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["monthly", "statements"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    statements_monthly_nigeria_ingest = PythonOperator(task_id='statements_monthly_nigeria_ingest',
                                  python_callable=statements_monthly_nigeria_ingest,
                                  op_kwargs={'message': 'Test Concept'},
                                  )
    # statements_tag_combine_transactions = TriggerDagRunOperator(
    #             task_id="statements_tag_combine_transactions",
    #             trigger_dag_id="statements_tag_combine_transactions",
    #             )

    dummy_start >> statements_monthly_nigeria_ingest 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
