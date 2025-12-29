from utils import get_cred, open_sheet, select_worksheet, transforme_to_dataframe_pandas, cred_bigquery
from google.cloud import bigquery

def extract_data_from_drive(serv_account_google_sh):
    cred = get_cred(serv_account_google_sh)
    sheet = open_sheet(cred)
    worksheet = select_worksheet(sheet)
    return worksheet

def auth_bigquery(project_id, serv_account_bigquery):
    cred = cred_bigquery(serv_account_bigquery)

    client = bigquery.Client(
        project=project_id,
        credentials = cred
    )
    return client

def load_df_to_bigquery(client, table_id, df):
    job = client.load_table_from_dataframe(
        df,
        table_id
    )

    job.result()
    print('Upload sucessful!')