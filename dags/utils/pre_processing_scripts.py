from datetime import datetime, timedelta
import csv
import os
import pandas as pd
import re
import sys
import xlrd
import msoffcrypto
import io
def ghipssGenerateDateFolders():
    min_date_str = '2023-07-25' #In Future this has to change
    min_date = datetime.strptime(min_date_str, '%Y-%m-%d')
    today = datetime.now()

    # Ensure min_date is before today
    min_date = min(today, min_date)

    # Generate dates between min_date and today
    date_list = []
    current_date = today
    while current_date >= min_date:
        date_list.append(current_date.strftime('%Y%m%d'))
        current_date -= timedelta(days=1)

    return date_list

def remove_preambles(final_local_path_with_file_name=None,num_of_columns=None,first_col_header=None):
    with open(final_local_path_with_file_name,'r',errors='ignore') as fil_data:
        data = list(csv.reader(fil_data))
        print(data)
    req_data = [x for x in data if len(x) >= num_of_columns]

    df = pd.DataFrame(req_data[1:],columns=req_data[0])
    df = df[~df[first_col_header].str.match(first_col_header,case=False) ]
    df.to_csv(final_local_path_with_file_name,quoting=csv.QUOTE_ALL,index=False)

def remove_preambles_2(final_local_path_with_file_name=None):
    # Input CSV file name
    input_filename = final_local_path_with_file_name
    # Output CSV file name
    output_filename = input_filename + '.csv'
    # Pattern to search for
    starts_with = ['NIBSS','TITAN','TRANSACTION','GENERATED','SOURCE >>','Paystack','PAYSTACK','DESTINATION >>','ACTIVITY REPORT']
    special_cases = ['SOURCE INSTITUTION ','SOURCE BANK SUMMARY','FULL BILLING','DESTINATION BANK SUMMARY','DESTINATION INSTITUTION ','FULL DAY SUMMARY'] 
    header_starts_with = 'S/N'
    
    with open(input_filename, 'r',errors='ignore') as infile:
        lines = infile.readlines()
    
    skip_lines = 0
    header_found = False
    with open(output_filename, 'w', newline='') as outfile:
        for line in lines:
            if skip_lines > 0:
                skip_lines -= 1
                continue
            if not line.strip():
                continue  
            if line.startswith(header_starts_with) or line.startswith('"' + header_starts_with):
                if header_found:
                    continue
                else:
                    header_found = True
                    
            starts_with_match = False
            for prefix in starts_with:
                if line[0] == '"':
                    prefix = '"' + prefix
                if line.startswith(prefix):
                    starts_with_match = True
                    break
            if starts_with_match:
                continue
            special_case = False
            for prefix in special_cases:
                if line[0] == '"':
                    prefix = '"' + prefix
                if line.startswith(prefix):
                    special_case = True
                #continue 3 lines
            if special_case:     
                skip_lines = 2  # Set to 3 to skip the next 3 lines (including the current one)
                continue
            if 'NET SETTLEMENT POSITION :' in line:
                continue
            #print(line)
            outfile.write(line)
    os.rename(output_filename,input_filename)


def manage_pre_processing_script_param(pre_processing_script, **kwargs):
    # Extract substrings within and outside parentheses
    pattern = r'([^(]*)\((.*?)\)'
    matches = re.match(pattern, pre_processing_script)
    if matches:
        func = matches.group(1).strip()
        old_params = matches.group(2).strip().split(',')

    params = []
    for old_param in old_params:
        name, value = old_param.split('=')
        if value == 'None':
            value = "'" + kwargs.get(name, value) + "'"
        # if value == 'None':
        #     value = '{{{}}}'  # Placeholder for formatting later
        params.append(name + '=' + value)

    return '{}({})'.format(func, ','.join(params))

def combine_excel_sheets_using_pandas(final_local_path_with_file_name=None):
    #This is specific to zenith_gh statement report
    fil_path = final_local_path_with_file_name
    xl_file = pd.ExcelFile(fil_path)
    cnt = 0
    for act_no in xl_file.sheet_names:
        df = pd.read_excel(xl_file,sheet_name=act_no,skiprows=4)
        df.rename( columns={'Unnamed: 0':'sheet_row_number'}, inplace=True )
        df['act_no'] = act_no
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_')
        df = df[df['particulars'] != 'TOTALS']
        if cnt == 0:
            df_main = df.copy()
        else:
            df_main = pd.concat([df_main,df])
        cnt += 1
    df_main.to_excel(final_local_path_with_file_name,index=False)


def convert_html_read_using_pandas(final_local_path_with_file_name=None):
    try:
        base_name, extension = os.path.splitext(final_local_path_with_file_name)
        html_file_name = base_name + '.html'
        os.rename(final_local_path_with_file_name,html_file_name)
        df = pd.read_html(html_file_name,attrs = {'id': 'dgtrans'})[0]
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.reset_index(drop=True)
        df.to_excel(final_local_path_with_file_name,index=False)

    except Exception as err:
        print(f'Function convert_html_read_using_pandas failed with error \n {err}')
        sys.exit(1)

def for_zenith_statement_xlsx(final_local_path_with_file_name=None):
    try:
        df = pd.read_excel(final_local_path_with_file_name)
        # Find the index of the first row with all NaN values (excluding the first column)
        empty_row_index = df.iloc[:, 1:].apply(lambda row: row.isna().all(), axis=1).idxmax()
        # Slice the DataFrame up to the empty row
        df = df.iloc[:empty_row_index]

        df = df.rename(columns={"Create Date": "DATE", "Effective Date": "VALUE DATE", "Description/Payee/Memo": "PARTICULARS"})
        df.columns = df.columns.str.lower()
        df = df[['date','particulars','debit','credit','value date','balance']]
        df.to_excel(final_local_path_with_file_name,index=False)
    except Exception as err:
        print(f'Function for_zenith_statement_xlsx failed with error \n {err}')
        sys.exit(1)

def for_zenith_statement_xls(final_local_path_with_file_name=None):
    base_name, extension = os.path.splitext(final_local_path_with_file_name)
    account_number = base_name.split(' ')[1]
    passwd = account_number[-5:-1]
    html_file_name = base_name + '.html'
    try:
        df = pd.read_excel(final_local_path_with_file_name)
    except ValueError as e:
        if 'Excel file format cannot be determined, you must specify an engine manually.' in str(e):     
            os.rename(final_local_path_with_file_name,html_file_name)
            df = pd.read_html(html_file_name)[0]
            # Find the index of the first row with all NaN values (excluding the first column)
            empty_row_index = df.iloc[:, 1:].apply(lambda row: row.isna().all(), axis=1).idxmax()
            df = df.iloc[:empty_row_index]
            df = df.rename(columns={"Create Date": "DATE", "Effective Date": "VALUE DATE", "Description/Payee/Memo": "PARTICULARS"})
            df.columns = df.columns.str.lower()
            df = df[['date','particulars','debit','credit','value date','balance']]
            df.to_excel(final_local_path_with_file_name,index=False)
        #df.to_excel(final_local_path_with_file_name,index=False)
    except xlrd.XLRDError as e:
        if "Workbook is encrypted" in str(e):
            # Catch the specific XLRDError when the workbook is encrypted and handle it
            print("Workbook is encrypted. passing password.")
            decrypted_workbook = io.BytesIO()
            with open(final_local_path_with_file_name, 'rb') as xls_file:
                office_file = msoffcrypto.OfficeFile(xls_file)
                office_file.load_key(password=passwd)
                office_file.decrypt(decrypted_workbook)
            df = pd.read_excel(decrypted_workbook)
            df = df[8:]
            df.columns = df.iloc[0]
            df = df[1:]
            df = df.reset_index(drop=True)
            df.columns = df.columns.str.lower()
            df = df[['date','particulars','debit','credit','value date','balance']]
            total_index = df[df['particulars'] == 'TOTALS'].index
            df = df.iloc[:total_index[0]]
            df.to_excel(final_local_path_with_file_name,index=False) 
            
        else:
            # Handle other XLRDError cases
            print(f"An XLRDError occurred: {e}")
            sys.exit(1)
    except Exception as err:
            print(f'Function for_zenith_statement_xls failed with error \n {err}')
            sys.exit(1)

