import numpy as np
import pandas as pd
import polars as pl
import os, sys, hashlib, logging


def create_df(file:str):
    df = pd.read_excel(file, engine='openpyxl')
    # drop หมายเหตุ and NaN
    try:
        try:
            df = df.drop(columns='หมายเหตุ') #,'ทั้งหมด'])

            # df = df.drop(columns='ทั้งหมด')
            idx = df[df.isnull().all(axis=1)].index
            df = df.iloc[:idx[0],:]
        except:
            df = df.drop(columns='หมายเหตุ')
    except:
        try:
            print('ไม่มีหมายเหตุ ลบแถวว่าง')
            # df = df.drop(columns='ทั้งหมด')
            idx = df[df.isnull().all(axis=1)].index
            df = df.iloc[:idx[0],:]
        except:
            print('ไม่มีหมายเหตุ ไม่มีแถวว่าง')

    
    # the names to str 
    for col in df.iloc[:1,:7].columns.tolist():
        df[col] = df[col].astype(str)

        try:
            df[col] = df[col].strip('\t')
        except:
            continue

    if df.loc[3,'ระดับกระทรวง']== 'กระทรวงกระทรวงคมนาคม':
        df.at[3,'ระดับกระทรวง'] = 'กระทรวงคมนาคม'
    if df.loc[55,'ระดับกรม'] == '-':
        df.at[55,'ระดับกรม'] = 'กรุงเทพมหานคร'

    df['hash_app'] = df['ชื่อแอพ'].str.encode(encoding='utf-8')
    df['hash_app'] = df['hash_app'].apply(lambda x:hashlib.sha256(x).hexdigest())
    df = df.set_index('hash_app').reset_index()
    # after the names are all str astype amount from float to int
    col_num = df.select_dtypes(float).columns
    df[col_num] = df[col_num].astype(int)
    
    return df

def create_meta(df):
    
    df = df.select_dtypes(exclude = 'number')
    df = df.drop_duplicates()
    df = df.set_index('hash_app').reset_index()

    return df

# df is exclude number part
def check_new_app(meta, df):

    col = []
    for i in df:
        if i not in meta:
            # print(f'the columns {i} not in original metadata relation')
            col.append(i)
        else:
            continue

    if len(col) != 0:
        raise f'columns do not match, have {col} from new file'

    metaapp = meta[['hash_app','รหัสแอพ']].values.tolist()
    dfapp = df[['hash_app','รหัสแอพ']].values.tolist()

    # new_df_meta = create_meta(df)
    all_new = pd.DataFrame([], columns=df.columns)

    for app in dfapp:
        if app not in metaapp:
            newapp = df[(df['hash_app']==app[0]) & (df['รหัสแอพ']==app[1])]
            all_new = pd.concat([all_new,newapp], axis = 0)
    
    return all_new

def detect_new_month(old_num, df):

    old_m = old_num['month'].unique()
    new_m = df.iloc[:1,8:].columns

    new_month = []
    for m in new_m:
        if m not in old_m:
            new_month.append(m)
    
    return new_month
    

def split_new(df,new_month):

    df = pd.DataFrame(df.set_index(['hash_app','รหัสแอพ']).iloc[:,new_month])
    df = pd.DataFrame(df.stack())
    
    df.index.names = ['hash_app','app_code','month']
    df = df.rename(columns={0:'number'})

    zero = df[df['number']==0].index
    df = df.drop(index=zero)
    
    return df.reset_index()

def group_by_sum(old_split, df):

    old_split = pd.concat([old_split,df], axis = 0)
    old_split = old_split.groupby(['hash_app','app_code','month']).sum()

    # old_split.to_excel('external/numberdata.xlsx')

    return old_split

'''
def write_db(df):
    pass
'''


if __name__ == "__main__":
    df = create_df('for_level_dictionary.xlsx')
    df.iloc[0:0]

    # print(create_df("for_level_dictionary.xlsx"))

