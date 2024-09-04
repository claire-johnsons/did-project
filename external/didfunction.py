import numpy as np
import pandas as pd
# import polars as pl
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

    # after the names are all str astype amount from float to int
    col_num = df.select_dtypes(float).columns
    df[col_num] = df[col_num].astype(int)
    
    return df

def create_meta(df):
    
    df = df.select_dtypes(exclude = 'number')
    df = df.drop_duplicates()

    return df

def check_new_app(meta, df):

    metaapp = meta[['hash_app','รหัสแอพ']].values.tolist()
    dfapp = df[['hash_app','รหัสแอพ']].values.tolist()

    # col_for_add = meta.columns.tolist()

    new_df_meta = create_meta(df)

    for app in dfapp:
        if app not in metaapp:
            newapp = df[(df['ชื่อแอพ']==app[0]) & (df['รหัสแอพ']==app[1])]
            meta = pd.concat([meta,newapp], axis = 0)
    
    return meta





if __name__ == "__main__":
    df = create_df('for_level_dictionary.xlsx')
    df.iloc[0:0]

    # print(create_df("for_level_dictionary.xlsx"))

