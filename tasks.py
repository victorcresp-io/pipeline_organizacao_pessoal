from utils import get_cred, open_sheet, select_worksheet, transforme_to_dataframe_pandas

def extract_data_from_drive():
    cred = get_cred()
    sheet = open_sheet(cred)
    worksheet = select_worksheet(sheet)
    return worksheet