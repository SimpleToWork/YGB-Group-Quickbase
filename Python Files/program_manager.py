
from global_modules import print_color, run_sql_scripts, engine_setup, ProgramCredentials, create_folder
from pull_and_push_data import upload_product_data, upload_sales_data, upload_factory_pos, upload_shipment_data, \
    upload_shipment_detail_data, upload_shipment_tracking, upload_inventory_data, upload_settlement_fees, \
    upload_finance_fees, upload_sales_fees_data, factory_order_assignments, import_factory_pos, upload_returns_data
from google_sheets_api import GoogleSheetsAPI
import getpass
import platform
import datetime
import pandas as pd


def google_sheet_update(project_folder, program_name, method):
    text_folder = f'{project_folder}\\Text Files'
    create_folder(text_folder)
    client_secret_file = f'{project_folder}\\Text Files\\client_secret.json'
    token_file = f'{project_folder}\\Text Files\\token.json'
    sheet_id = '19FUWyywrtS4JTbOHW_GqDSEl0orqu99XCJJFa4upVlw'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    GsheetAPI = GoogleSheetsAPI(credentials_file=client_secret_file, token_file=token_file, scopes=SCOPES, sheet_id=sheet_id)

    computer_name = platform.node()
    user = getpass.getuser()
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_list = [time_now, computer_name, user, program_name, method, True]
    sheet_name = 'YGB Group'

    GsheetAPI.insert_row_to_sheet(sheetname=sheet_name, gid=2126035413,
                                  insert_range=['A', 1, "F", 1],
                                  data=[data_list])


def executeScriptsFromFile(engine, folder_name, file_name):
    # Open and read the file as a single buffer
    fd = open(f'{folder_name}\\{file_name}', 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')[1:]

    sqlCommands = [x for x in sqlCommands if x.replace("\n","") != '']
    print(sqlCommands)

    run_sql_scripts(engine=engine, scripts=sqlCommands)
    print_color(f'Sql File {file_name} Exceuted', color='g')


def run_program(environment):
    x = ProgramCredentials(environment)
    sql_folder = f'{x.project_folder}\\Sql Files'
    start_date = "2023-01-01"
    po_start_date = "2022-11-01"
    engine = engine_setup(project_name=x.project_name, hostname=x.hostname, username=x.username, password=x.password, port=x.port)
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='data logic.sql')
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='finances logic.sql')
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='sales logic.sql')



    upload_product_data(x, engine)
    upload_sales_data(x, engine, start_date)
    upload_returns_data(x, engine, start_date)

    upload_sales_fees_data(x, engine, start_date)

    upload_settlement_fees(x, engine, start_date)
    upload_finance_fees(x, engine)

    upload_shipment_data(x,engine)
    upload_shipment_detail_data(x, engine)
    upload_shipment_tracking(x, engine)

    upload_inventory_data(x, engine)
    upload_factory_pos(x, engine, po_start_date)
    import_factory_pos(x, engine)
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='ledger logic.sql')

    factory_order_assignments(x, engine)
    google_sheet_update(project_folder=x.project_folder, program_name="YGB Group", method="Run Program")


if __name__ == '__main__':

    environment = 'development'
    run_program(environment)