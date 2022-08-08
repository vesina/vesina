import csv
import sqlalchemy as sa
from sqlalchemy import create_engine, engine
from sqlalchemy import text
# import config_constants as cfg_const
# import config as cfg
import logging

import pandas as pd
import sys
import os
import getopt
import re
import numpy as np

DEFAULT_LOGGER = 'crs-dev'
USER_ID = None
 
QUESTIONS = '../../seperated_question2u.csv'
COMMENTERS = '../../commenters_list.csv'
CP_ID = 8

def format_qname(fmt_str: str, row: [], column_ids: [], delim: str=" ") -> str:

    result = []
    fmtstr = 'Q {0} - {1}' if fmt_str == '' else fmt_str
    fmt_array = fmtstr.split(delim)
    vals = get_values(row,column_ids)
    if isinstance(vals[0], (bool, int)) or fmt_str == '' or fmt_str == '{0}':
        return vals[0]

    i = 0
    for itm in fmt_array:
        if re.search('{', itm):
            result.append(itm.format(vals[i]))
            i = i + 1
        else:
            result.append(itm)
    return delim.join(result)

def format_name(fmt_str: str, vals: [], delim: str=" "):
    fmt_array = fmt_str.split(delim)
    result = []
    i = 0
    for itm in fmt_array:
        if re.search('{', itm):
            result.append(itm.format(vals[i]))
            i = i + 1
        else:
            result.append(itm)
    return delim.join(result)

def get_values(row, cols: []):
    values = []
    for id in cols:
        values.append(row[id].replace('\ufeff', ''))
    return values
            
def df_add_mising_cols(df, num: int):
    cur = len(df)
    if cur < num:
        for n in range(num - cur):
            df[f'col{cur+n}'] = ""
    return df

def read_csv_to_list_multiline(file: str, col_list: list=[], col_fmt: list=[], userid: int=None):
    columns = col_list
    colfmt = col_fmt
    lst = []
    with open(file, newline='', encoding='utf-8-sig') as fl:
        reader = csv.reader(fl, delimiter=',', quotechar='"')
        
        for row in reader: 
            print(row)
            # s = row[2].replace('\ufeff', '')     
            # lst.append([colfmt[0].format(CP_ID) 
            #     , format_qname(colfmt,row,[2,0])
            #     , colfmt[2].format(row[1])
            #     , colfmt[3].format(row[2])
            #     , colfmt[4].format(row[3])
            #     , True
            #     , colfmt[4].format(userid)])
            lst.append([CP_ID
                , colfmt[1].format(row[0]) if colfmt[1] == '{0}' else format_qname(colfmt[1],row,[2,0])
                , row[3]
                , True
                , userid])
    df = pd.DataFrame(lst)
    df.columns = columns        
    print(df.columns)    
     
    df['label_description'] = df.groupby(["label_name"])["description"].transform(lambda x : '\n'.join(x))
    dfnew = df.drop(["description"], axis=1)
    dfnew = dfnew.drop_duplicates()   
    print(dfnew)
    return dfnew

def get_label_maxid(engine: engine):
    stmt = f"select max(convert(int,replace(label_name,'question ', ''))) as num from dbo.cra_label where label_name like 'Question%[0-9]%'"
    maxid = engine.execute(stmt).first()
    print(f'Max ID: {maxid}')
    if not maxid or maxid[0] is None:
        return 0
    else: 
        return maxid['num']

def get_user_id(eng: engine, usrid: str=USER_ID) -> int:
    check_stmt = f"SELECT TOP (1) [id] FROM [dbo].[cra_user_info] where [user_id] = nullif('{usrid}','')"
    res = eng.execute(check_stmt).first()
    print(f'RES: {res}')
    if res:
        uid = res['id']
    else:
        uid=None
    return uid

# def upload_labels_df(df: pd.DataFrame, eng: engine, check_col: str, maxid: str=''):
#     results = []
#     if len(df) == 0:
#         print('No labels to load')
#     else:
#         df = df.reset_index()  # make sure indexes pair with number of rows
#         usr_id = get_user_id(eng, USER_ID)
#         print(f"USER_ID: {usr_id}")

#         for index, row in df.iterrows():
#             check_stmt = f"select top (1) id from dbo.cra_label where {check_col} = '{row[f'{check_col}']}'"
#             print(check_stmt)
#             res = eng.execute(check_stmt).first()
#             cp_id = CP_ID # row['cp_id']            

#             label_name = row['label_name']
#             label_description = row['label_description']
#             is_global = True # row['is_global']
#             if not res:
#                 print(f"None: {check_col}")
#                 stmt = f"""insert dbo.cra_label (cp_id, label_name, label_description,is_global,user_id) 
#                 values({cp_id},'{label_name}','{label_description}',{is_global},{usr_id})"""
#                 try:
#                     eng.execute(stmt)
#                     msg = f'{index}: Created: {check_col}' 
#                 except:
#                     msg = f'{index}: Failed to create: {check_col}'   
#                     print(stmt)                 
#             else:
#                 msg = f'Exists {res[0]}: {check_col}'
#             results.append(msg)
#             # n += 1
#     return results

# def file_to_table(file: str, columns: list=[], colfmt: list=[], check_col: str='label_name'):
#     eng = sa.create_engine(sa.url, echo=True)
#     userid = get_user_id(eng=eng, usrid=USER_ID)
#     df = read_csv_to_list_multiline(file=file, col_list=columns, col_fmt=colfmt)
#     results = upload_labels_df(df=df, eng=eng, check_col=check_col)
#     for res in results:
#         print(res)


 

 
