
from global_modules import print_color, run_sql_scripts, engine_setup, ProgramCredentials, create_folder
from pull_and_push_data import upload_product_data, upload_sales_data, upload_factory_pos, upload_shipment_data, \
    upload_shipment_detail_data, upload_shipment_tracking, upload_inbound_tracking_data, upload_inventory_data, upload_settlement_fees, \
    upload_finance_fees, upload_sales_fees_data, factory_order_assignments, import_factory_pos, upload_returns_data, \
    import_quickbase_product_data, import_quickbase_order_data, upload_document_links
from google_sheets_api import google_sheet_update
import getpass
import platform
import datetime
import pandas as pd




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
    seller_sql_folder = f'{x.sql_folder}'
    sql_folder = f'{x.project_folder}\\sql files'
    start_date = "2023-01-01"
    po_start_date = "2022-11-01"
    engine = engine_setup(project_name=x.project_name, hostname=x.hostname, username=x.username, password=x.password, port=x.port)

    executeScriptsFromFile(engine=engine, folder_name=seller_sql_folder, file_name='1. data logic.sql')
    executeScriptsFromFile(engine=engine, folder_name=seller_sql_folder, file_name='2. finances logic.sql')
    executeScriptsFromFile(engine=engine, folder_name=seller_sql_folder, file_name='3. sales logic.sql')
    executeScriptsFromFile(engine=engine, folder_name=seller_sql_folder, file_name='4. Settlement Logic.sql')

    upload_product_data(x, engine)
    upload_sales_data(x, engine, start_date)
    upload_returns_data(x, engine, start_date)

    upload_sales_fees_data(x, engine, start_date)

    upload_settlement_fees(x, engine, start_date)
    upload_finance_fees(x, engine)

    upload_shipment_data(x,engine)
    upload_shipment_detail_data(x, engine)
    upload_shipment_tracking(x, engine)
    upload_inbound_tracking_data(x, engine)

    upload_inventory_data(x, engine)
    upload_factory_pos(x, engine, po_start_date)

    import_factory_pos(x, engine)
    upload_document_links(x, engine)

    import_quickbase_product_data(x, engine)
    import_quickbase_order_data(x, engine)

    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='ledger logic.sql')
    factory_order_assignments(x, engine)
    google_sheet_update(x=x, program_name="YGB Group", method="Run Program")


if __name__ == '__main__':

    environment = 'development'
    run_program(environment)