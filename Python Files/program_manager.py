
from global_modules import print_color, run_sql_scripts, engine_setup, ProgramCredentials
from pull_and_push_data import upload_product_data, upload_sales_data

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
    engine = engine_setup(project_name=x.project_name, hostname=x.hostname, username=x.username, password=x.password, port=x.port)
    executeScriptsFromFile(engine=engine, folder_name=sql_folder, file_name='data logic.sql')
    upload_product_data(x, engine)
    # upload_sales_data(x, engine)


if __name__ == '__main__':
    environment = 'development'
    run_program(environment)