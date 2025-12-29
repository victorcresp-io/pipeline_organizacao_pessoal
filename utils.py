import os

import pandas as pd
import gspread
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

project_id = os.getenv('PROJECT_ID')
sheet_name = os.getenv('SHEET_NAME')
target_worksheet = os.getenv('TARGET_WORKSHEET')


def get_cred(serv_account_google_sh):
    cred = gspread.service_account(filename=serv_account_google_sh)
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

def cred_bigquery(serv_account_bigquery):
    cred = service_account.Credentials.from_service_account_file(serv_account_bigquery)
    return cred



if __name__ == '__main__':
    client_bq = auth_bigquery()
    query = "SELECT CURRENT_DATE() AS today"

    result = client_bq.query(query).result()

    for row in result:
        print(row.today)