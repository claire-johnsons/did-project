import numpy as np
import pandas as pd
import polars as pl
import os, sys, hashlib, logging
from string import punctuation


# สร้าง df จาก excel และ clean
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

    
    # 7 col แรกเปลี่ยน value เป็น str และ strip \t เพื่อเตรียมใช้เป็น metadata 
    for col in df.iloc[:1,:7].columns.tolist():
        if df[col].dtypes == float:
            df[col] = df[col].astype(int).astype(str)
            # df[col] = df[col].astype(str)
        else:
            df[col] = df[col].astype(str)

        try:
            df[col] = df[col].strip('\t')
        except:
            continue

    # hashing
    df['hash_app'] = df['ชื่อแอพ'].str.encode(encoding='utf-8')
    df['hash_app'] = df['hash_app'].apply(lambda x:hashlib.sha256(x).hexdigest())
    df = df.set_index('hash_app').reset_index()

    # เปลี่ยน value ส่วนจำนวนผู้ใช้งานจาก float -> int
    col_num = df.select_dtypes(float).columns
    df[col_num] = df[col_num].astype(int)
    
    # ค่า 7 col แรก =str ที่เหลือเป็น int
    return df

# เลือกเฉพาะ col ที่ไม่ใช่ number
def create_meta(df):
    
    df = df.select_dtypes(exclude = 'number')
    df = df.drop_duplicates()
    # เปลี่ยนเว้นวรรคเป็นค่า '-' 
    for i in df:
        for j in df[i].index:
            # check ว่า value เป็นตัวอักษรพิเศษหรือเว้นวรรคไหน ถ้าใช่เปลี่ยนเป็น '-'
            if df.loc[j,i] in list(punctuation)+[' ',None]:
                df.at[j,i] = '-'

    df = df.set_index('hash_app').reset_index()

    return df

# input คือ metadata เก่าและใหม่ตามลำดับ
def check_new_app(meta, df):

    # check ก่อนว่ามี col ไหนที่ไม่เคยมีอยู่ใน metadata เก่าบ้าง
    col = []
    for i in df:
        if i not in meta:
            # print(f'the columns {i} not in original metadata relation')
            col.append(i)
        else:
            continue

    # ถ้ามี col ใหม่ให้ raise
    if len(col) != 0:
        raise f'columns do not match, have {col} from new file'

    metaapp = meta[['hash_app','รหัสแอพ']].values.tolist()
    dfapp = df[['hash_app','รหัสแอพ']].values.tolist()

    # new_df_meta = create_meta(df)
    all_new = pd.DataFrame([], columns=meta.columns)

    # check value แถวต่อแถว ถ้า not in meta เดิมแปลว่าเป็นแอปใหม่
    # จะ loop concat แอปใหม่ไปเรื่อย ๆ 
    for app in dfapp:
        if app not in metaapp:
            newapp = df[(df['hash_app']==app[0]) & (df['รหัสแอพ']==app[1])]
            all_new = pd.concat([all_new,newapp], axis = 0)
    
    # ได้ df ที่มีแต่แอปใหม่
    return all_new

# input คือตารางจำนวนเก่าจาก db และ df ที่พึ่งสร้างจาก excel
def detect_new_month(old_num, df):

    old_m = old_num['month'].unique()
    new_m = df.iloc[:1,8:].columns

    # ถ้าเลขเดือน not in เลขเดือนเก่าให้ append ใส่ list
    new_month = []
    for m in new_m:
        if m not in old_m:
            new_month.append(m)

    # ถ้าไม่มีเดือนใหม่ raise
    if new_month == []:
        raise 'no new month'
    
    # เอาชื่อคอลัมน์ทั้งหมดออก (ถ้ามี) เพราะ new_month จะเอาไปใช้ต่อ
    try:
        new_month.remove('ทั้งหมด')
    except:
        pass
    
    return new_month
    
# สร้างและ manipulate ตารางจำนวนโดย input คือ df จาก excel
def split_new(df, new_month):

    # loc มาเฉพาะคอลัมน์ใน new_month และใช้ hash กับรหัสแอพเป็นคีย์
    df = pd.DataFrame(df.set_index(['hash_app','รหัสแอพ']).loc[:,new_month])
    df = pd.DataFrame(df.stack())
    
    # เปลี่ยนชื่อคอลัมน์
    df.index.names = ['hash_app','app_code','month']
    df = df.rename(columns={0:'number'})

    # drop column ที่มีค่าเป็น 0 สามารถคอมเมนท์ไว้ถ้าไม่ต้องการ drop
    zero = df[df['number']==0].index
    df = df.drop(index=zero)
    
    return df.reset_index()


# ไม่ได้ใช้
def group_by_sum(old_split, df):

    old_split = pd.concat([old_split,df], axis = 0)
    old_split = old_split.groupby(['hash_app','app_code','month']).sum()

    # old_split.to_excel('external/numberdata.xlsx')

    return old_split


'''
# ใช้ลองรัน
if __name__ == "__main__":
    df = create_df('for_level_dictionary.xlsx')
    df.iloc[0:0]

    # print(create_df("for_level_dictionary.xlsx"))
'''
