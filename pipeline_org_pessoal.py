import os

from prefect import flow, task
from tasks import extract_data_from_drive
from utils import transforme_to_dataframe_pandas

@task
def extract_data():
    worksheet = extract_data_from_drive()
    return worksheet

@task
def transform(worksheet):
    df = transforme_to_dataframe_pandas(worksheet)
    return df

@flow()
def etl_pipeline():
    data = extract_data()
    processed = transforme_to_dataframe_pandas(data)