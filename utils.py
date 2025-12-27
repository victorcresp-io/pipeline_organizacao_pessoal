import os

import pandas as pd
import gspread
from dotenv import load_dotenv

load_dotenv()

serv_account = os.getenv('SERV_ACCOUNT')
sheet_name = os.getenv('SHEET_NAME')
target_worksheet = os.getenv('TARGET_WORKSHEET')


def get_cred():
    cred = gspread.service_account(filename=serv_account)
    return cred

def open_sheet(cred):
    sheet = cred.open(sheet_name)
    return sheet

def select_worksheet(sheet):
    worksheet = sheet.worksheet(target_worksheet)
    return worksheet

def transforme_to_dataframe_pandas(worksheet):
    df = pd.DataFrame(worksheet.get_all_records())
    return df

def transforme_dataframe_columns(df):
    df.columns = [
    "despesas",
    "valor",
    "dia",
    "responsavel",
    "motivo_despesa",
    "itens"
    ]
    return df

def testing():
    cred = get_cred()
    sheet = open_sheet(cred)
    worksheet = select_worksheet(sheet)
    df = transforme_to_dataframe_pandas(worksheet)
    df_tratado = transforme_dataframe_columns(df)
    return df

if __name__ == '__main__':
    df = testing()
    print(df.info())