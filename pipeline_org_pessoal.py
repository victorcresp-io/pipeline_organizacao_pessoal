import os

from google.cloud import bigquery
from prefect import flow, task
from tasks import extract_data_from_db, auth_bigquery, load_df_to_bigquery
from utils import transforme_to_dataframe_pandas, cred_bigquery
from dotenv import load_dotenv


load_dotenv()

serv_account_sh = os.getenv('SERV_ACCOUNT')
serv_account_bigquery = os.getenv('SERV_ACCOUNT_BIGQUERY')
project_id = os.getenv('PROJECT_ID')
table_id = os.getenv('TABLE_ID')
path_db_prod = os.getenv('PATH_DB_PROD')

@task
def extract_data(path_db_prod):
    worksheet = extract_data_from_db(path_db_prod)
    return worksheet

@task
def load_to_bigquery(serv_account_bigquery, df, table_id):
    client = auth_bigquery(project_id, serv_account_bigquery)
    load_df_to_bigquery(client, table_id, df)

@flow()
def etl_pipeline():
    df = extract_data(path_db_prod)
    load_to_bigquery(serv_account_bigquery, df, table_id)

