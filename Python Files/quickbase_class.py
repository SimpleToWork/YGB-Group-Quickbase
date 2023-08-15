from global_modules import print_color
import requests
import datetime
import json
import pandas as pd

class QuickbaseAPI():
    def __init__(self, hostname, auth, app_id):
        self.headers = {
            'QB-Realm-Hostname': hostname,
            'User-Agent': 'Windows NT 10.0; Win64; x64',
            'Authorization': f'QB-USER-TOKEN {auth}'
        }

        self.hostname = hostname
        self.auth = auth
        self.app_id = app_id

    def get_variable_value(self,apptoken=None, username=None, password=None, table_id=None, variable_value=None):
        r = requests.get(f'{self.hostname}/db/main?a=API_Authenticate&username={username}&password={password}&hours=24')
        ticket = str(r.content).split('</ticket>')[0].split('<ticket>')[-1]
        # print_color(ticket, color='y')

        url = f'{self.hostname}/db/{table_id}?a=API_GetDBVar&ticket={ticket}&apptoken={apptoken}&varname={variable_value}'
        # print(url)
        r = requests.get(url)
        value = str(r.content).split('</value>')[0].split('<value>')[-1]
        print_color(f'Variable Value: {variable_value}:{value}', color='p')

        return value

    def get_qb_table_records(self,  table_id=None,  col_order='*', filter=None, field_id=3, filter_type='CT', filter_operator=None):

        print_color(f'Recruiting Data For Table_id {table_id}', color='r')
        params = {
            'tableId': table_id,
        }
        r = requests.get(
            'https://api.quickbase.com/v1/fields',
            params=params,
            headers=self.headers
        )

        fields_data = r.json()
        # print_color(fields_data, color='y')
        # print(type(fields_data))
        # print(fields_data)

        field_dict = {}

        for each_item in fields_data:
            # print_color(each_item, color='g')
            # print_color(each_item.keys(), color='r')
            id = each_item.get('id')
            fieldType = each_item.get('fieldType')
            label = each_item.get('label')
            field_dict.update({id: [label, fieldType]})

        print_color(type(filter), color='r')


        if filter is None:
            body = {
                "from": table_id,
                "select": col_order}
        elif type(filter) == str:
            # if filter_type == "CT":
            where_setting = '{' + str(field_id) + f".{filter_type}.'" + str(filter) + "'}" + ''
            print_color(f'Fiter Setting: {where_setting}', color='b')
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }

        elif type(filter) == bool:
            where_setting = '{' + str(field_id) + f".{filter_type}.'" + str(filter) + "'}" + ''
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }
        elif type(filter) == list:
            print_color(filter, color='g')
            where_setting = ''
            for i, j in enumerate(filter):
                if j == "True":
                    val = True
                elif j == "False":
                    val = False
                else:
                    val = j



                if i == 0:
                    where_setting = '{' + str(field_id[i]) + f".{filter_type[i]}.'" + str(val) + "'}" + ''
                else:
                    where_setting += f'\n{filter_operator[i-1]} ' + '{' + str(field_id[i]) + f".{filter_type[i]}.'" + str(val) + "'}"

            print_color(f'Fiter Setting: {where_setting}', color='b')
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }

        print_color(f'Request Settings: {body}', color='g')
        r = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=self.headers,
            json=body
        )

        # print_color(r.json(), color='r')

        data = eval(json.dumps(r.json()).replace("null", "None").replace("false", "False").replace("true", "True")).get(
            "data")
        # print(data)
        df = pd.DataFrame(data)
        for each_col in df.columns:
            df[each_col] = df[each_col].apply(lambda x: x.get('value'))


        col_order_str = [str(x) for x in col_order]
        if df.shape[0] > 0:
            df = df[col_order_str]
            df_columns = list(df.columns)
            df_columns = [int(x) for x in df_columns]
            df.columns = [field_dict.get(x)[0].replace(" ", "_").replace("#", "_Num") for x in df_columns]

        all_columns = [x.lower() for x in df.columns]
        for i, col in enumerate(df.columns):
            print_color(f'{i}: Column ID {col_order[i]} ---- {col}', color='g')
            if col.lower() == 'date':
                df.rename(columns={col: 'QB_Date'}, inplace=True)

        column_dict = {}
        data_columns = df.columns

        if df.shape[0]>0:
            for j, id in enumerate(col_order):
                column_dict.update({id: data_columns[j]})



        df['Date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        return df, column_dict

    def update_qb_table_records(self,  table_id=None, data = [{}]):
        body = {"to": table_id, "data": data,
                "fieldsToReturn": [3]}

        print(body)

        r = requests.post(
            'https://api.quickbase.com/v1/records',
            headers=self.headers,
            json=body
        )

        print(json.dumps(r.json()))

        print_color(f'Record Updated', color='b')

    def delete_qb_table_records(self,  table_id=None, data = [{}]):
        body = data

        print(body)

        r = requests.delete(
            'https://api.quickbase.com/v1/records',
            headers=self.headers,
            json=body
        )

        print(json.dumps(r.json()))

        print_color(f'Records Deleted', color='b')


    def create_qb_table_records(self,  table_id=None, user_token=None, apptoken=None,
                                username=None, password=None,
                                filter_val=None, update_type=None, data=None,
                                reference_column=None
                                ):
        headers = self.headers

        r = requests.get(
            f'{self.hostname}/db/main?a=API_Authenticate&username={username}&password={password}&hours=24')
        ticket = str(r.content).split('</ticket>')[0].split('<ticket>')[-1]
        print_color(ticket, color='y')

        body = {"to": f'{table_id}', "data": data, "fieldsToReturn": ['3']}
        print_color(body, color='g')
        if update_type == 'purge_and_reset':
            print(self.hostname,
                  table_id,
                  reference_column,
                  filter_val,
                  ticket,
                  user_token,
                  apptoken)

            purge_records_url = f"{self.hostname}/db/" + table_id + "?a=API_PurgeRecords&query={" + str(reference_column) + ".EX.'" + str(filter_val) + "'}&ticket=" + ticket + "&user_token=" + user_token + "&apptoken=" + apptoken

            print_color(purge_records_url, color='g')
            r = requests.get(purge_records_url)
            print_color(r.content)

            print_color(f'Records Purged', color='b')


            r = requests.post(
                'https://api.quickbase.com/v1/records',
                headers=headers,
                json=body
            )
            print_color(r.content, color='g')
            print(json.dumps(r.json()))

            print_color(f'Record For Proposal {filter_val} Updated', color='b')

        elif update_type == 'add_record':
            r = requests.post(
                'https://api.quickbase.com/v1/records',
                headers=headers,
                json=body
            )

            print_color(r.content, color='g')
            print(json.dumps(r.json()))

            print_color(f'Record For Proposal {filter_val} Updated', color='b')
