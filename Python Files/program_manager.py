
from global_modules import print_color, run_sql_scripts, engine_setup, ProgramCredentials
from pull_and_push_data import upload_product_data, upload_sales_data, upload_factory_pos, upload_shipment_data, \
    upload_shipment_detail_data, upload_shipment_tracking, upload_inventory_data, upload_settlement_fees
from google_sheets_api import GoogleSheetsAPI
import getpass
import platform
import datetime
import pandas as pd


def google_sheet_update(project_folder, program_name, method, sheetname=None):
    client_secret_file = f'{project_folder}\\Text Files\\client_secret.json'
    token_file = f'{project_folder}\\Text Files\\token.json'
    sheet_id = '19FUWyywrtS4JTbOHW_GqDSEl0orqu99XCJJFa4upVlw'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    df = GoogleSheetsAPI(client_secret_file,token_file, sheet_id, SCOPES). get_data_from_sheet(sheetname=sheetname, range_name='A:E')
    print_color(df, color='r')

    row_number = df.shape[0] + 2

    computer_name = platform.node()
    user = getpass.getuser()
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data_list = [time_now, computer_name, user, program_name, method, True]
    df = pd.DataFrame([data_list])
    print_color(df)
    GoogleSheetsAPI(client_secret_file,token_file, sheet_id, SCOPES).write_data_to_sheet(data =df ,sheetname=sheetname,row_number=row_number,include_headers=False,  clear_data=False)



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
    start_date = "2022-01-01"
    engine = engine_setup(project_name=x.project_name, hostname=x.hostname, username=x.username, password=x.password, port=x.port)
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='data logic.sql')
    # upload_product_data(x, engine)
    # upload_sales_data(x, engine, start_date)
    # upload_settlement_fees(x, engine)
    # upload_shipment_data(x,engine)
    # upload_shipment_detail_data(x, engine)
    upload_shipment_tracking(x, engine)
    # upload_inventory_data(x, engine)
    # upload_factory_pos(x, engine)



if __name__ == '__main__':
    environment = 'development'
    run_program(environment)