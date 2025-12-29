import os

from prefect import flow, task
from tasks import extract_data_from_drive, auth_bigquery, load_df_to_bigquery
from utils import transforme_to_dataframe_pandas, cred_bigquery
from dotenv import load_dotenv

load_dotenv()

serv_account_sh = os.getenv('SERV_ACCOUNT')
serv_account_bigquery = os.getenv('SERV_ACCOUNT_BIGQUERY')
project_id = os.getenv('PROJECT_ID')
table_id = os.getenv('TABLE_ID')

@task
def extract_data():
    worksheet = extract_data_from_drive(serv_account_sh)
    return worksheet

@task
def transform(worksheet):
    df = transforme_to_dataframe_pandas(worksheet)
    return df

@task
def load_to_bigquery(serv_account_bigquery, df, table_id):
    client = auth_bigquery(project_id, serv_account_bigquery)
    load_df_to_bigquery(client, table_id, df)

@flow()
def etl_pipeline():
    data = extract_data()
    df = transform(data)
    load_to_bigquery(serv_account_bigquery, df, table_id)

etl_pipeline()