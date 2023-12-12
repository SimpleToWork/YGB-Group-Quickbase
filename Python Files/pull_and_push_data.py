import getpass
import pandas as pd
import numpy as np
from global_modules import print_color, run_sql_scripts, Get_SQL_Types, Change_Sql_Column_Types
from quickbase_class import QuickbaseAPI
from google_sheets_api import GoogleSheetsAPI
import datetime
from sqlalchemy import inspect
import re
from dateutil.parser import parse


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        if string is not None:
            parse(string, fuzzy=fuzzy)

        return True

    except ValueError:
        return False


def upload_product_data(x, engine):
    df = pd.read_sql(f'Select * from quickbase_product_data', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df = df.replace(np.nan, "")
    print_color(df, color='y')

    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    # df['SKU'] = df['SKU'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()

    quickbase_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )

    print_color(quickbase_data, color='r')
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    if quickbase_data.shape[0] >0:
        reference_df = quickbase_data[[account_name_column, sku_column, asin_column]]
        reference_df.columns = ['ACCOUNT_NAME', 'SKU', 'ASIN']
        print_color(reference_df, color='y')
        new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SKU', 'ASIN'], keep=False)
        # print(type(reference_df), type(df))
    else:
        new_df = df
        print_color(new_df, color='y')

    new_df['OPEN-DATE'] = pd.to_datetime( new_df['OPEN-DATE'])
    data = []
    print_color(new_df.columns, color='y')
    print_color(new_df.shape[0], color='y')


    new_df.fillna('', inplace=True)
    print_color(new_df, color='y')

    for i in range(new_df.shape[0]):
        account_name = new_df['ACCOUNT_NAME'].iloc[i]
        asin = new_df['ASIN'].iloc[i]
        parent_asin = new_df['PARENT_ASIN'].iloc[i]
        sku = new_df['SKU'].iloc[i]
        listing_id = new_df['LISTING-ID'].iloc[i]
        item_name = new_df['ITEM-NAME'].iloc[i]
        description = new_df['ITEM-DESCRIPTION'].iloc[i]
        price = new_df['PRICE'].iloc[i]
        open_date = new_df['OPEN-DATE'].iloc[i]
        print_color(open_date, color='r')
        if open_date != "" and  str(open_date) != "NaT":
            open_date =  open_date.strftime('%Y-%m-%dT%H:%M:%S')

        product_id = new_df['PRODUCT-ID'].iloc[i]
        fulfillment_channel = new_df['FULFILLMENT-CHANNEL'].iloc[i]
        status = new_df['STATUS'].iloc[i]
        style = new_df['STYLE'].iloc[i]
        color = new_df['COLOR'].iloc[i]
        size = new_df['SIZE'].iloc[i]
        fnsku = new_df['FNSKU'].iloc[i]

        body = {
            x.upload_data.product_fields.product_name: {"value": item_name},
            x.upload_data.product_fields.sku: {"value": sku},
            x.upload_data.product_fields.parent_asin: {"value": parent_asin},
            x.upload_data.product_fields.asin: {"value": asin},
            x.upload_data.product_fields.size: {"value": size},
            x.upload_data.product_fields.product_id: {"value": product_id},
            x.upload_data.product_fields.color: {"value": color},
            x.upload_data.product_fields.description: {"value": description},
            x.upload_data.product_fields.price: {"value": price},
            x.upload_data.product_fields.listing_id: {"value": listing_id},
            x.upload_data.product_fields.fnsku: {"value": fnsku},
            x.upload_data.product_fields.fulfillment_channel: {"value": fulfillment_channel},
            x.upload_data.product_fields.status : {"value": status},
            x.upload_data.product_fields.account_name: {"value": account_name},
            x.upload_data.product_fields.open_date: {"value": open_date},
            x.upload_data.product_fields.style: {"value": style},
        }
        data.append(body)
        # break

    print_color(data, color='b')
    if len(data) >0:
        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.product_table_id,
            user_token=x.qb_user_token, apptoken=x.qb_app_token, username=x.username, password=x.password,
            filter_val=None, update_type='add_record', data=data,
            reference_column=None)


def upload_sales_data(x, engine, start_date):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()

    QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.sales_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val='Sale',
            reference_column=x.upload_data.sales_fields.order_type,
            filter_type="EX"
        )
    # imported_sales_dates = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_report_records(
    #     table_id=x.sales_table_id, report_id=8)
    # print_color(imported_sales_dates, color='y')
    # if imported_sales_dates.shape[0] >0:
    #     imported_sales_dates['Date_Created_(max)'] = imported_sales_dates['Date_Created_(max)'].apply(lambda x: x.split("Z")[0])
    #     imported_sales_dates['Date_Created_(max)'] = pd.to_datetime(imported_sales_dates['Date_Created_(max)'], format="%Y-%m-%d %H:%M:%S")
    #
    #     imported_sales_dates['Date'] = pd.to_datetime(imported_sales_dates['Date'], format="%m-%d-%Y")
    #
    #     # imported_sales_dates['Date_Created_(max)'] = imported_sales_dates['Date_Created_(max)'].dt.tz_convert('US/Eastern')
    #     # print_color(imported_sales_dates, color='y')
    #     # print_color(list(imported_sales_dates['Date'].unique()))
    #     # # imported_sales_dates['Date'] = pd.datetime(imported_sales_dates['Date'])
    #
    #     dates_already_imported_today = imported_sales_dates[(imported_sales_dates['Date_Created_(max)'] >= datetime.datetime.now().date())]
    #     print_color(dates_already_imported_today, color='g')
    #
    #     max_date_imported = dates_already_imported_today['Date'].max()
    #     print(max_date_imported)
    #
    #     if str(max_date_imported) in ('nan', 'NaT') :
    #         date_to_delete_from = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%S')
    #     else:
    #         date_to_delete_from = max_date_imported + datetime.timedelta(days=1)
    #
    #
    #     print(date_to_delete_from)
    #
    #
    #
    # print_color(imported_sales_dates, color='y')
    # if imported_sales_dates.shape[0] >0:
    #     imported_sales_dates['Date'] = pd.to_datetime(imported_sales_dates['Date'], format="%m-%d-%Y").dt.date
    #     recruited_sales_dates = list(imported_sales_dates['Date'].unique())
    #     min_order_date = imported_sales_dates['Date'].max()
    # else:
    # recruited_sales_dates = []
    date_data = pd.read_sql(f'''Select min(date(`PURCHASE-DATE`)) as start_date,  max(date(`PURCHASE-DATE`)) as end_date 
        from ygb_quickbase_final_assigned_orders where `PURCHASE-DATE` >= "{start_date}"''', con=engine)

    min_order_date= date_data['start_date'].iloc[0]
    max_order_date = date_data['end_date'].iloc[0]
    print_color(min_order_date, color='y')
    print_color(max_order_date, color='y')

    delta = (max_order_date - min_order_date).days + 1
    print_color(delta, color='y')
    print_color(min_order_date, color='y')
    print_color(max_order_date, color='y')

    counter = 0
    # print_color(recruited_sales_dates, color='g')
    # print_color(imported_sales_dates, color='y')


    for i in range(delta):
        # if counter <= 0:
        date_to_recruit = min_order_date + datetime.timedelta(days = i)
        next_date_to_recruit = min_order_date + datetime.timedelta(days = i+1)
        print_color(date_to_recruit, color='g')
        # if date_to_recruit in recruited_sales_dates:
        #     print_color(f'Data for date {date_to_recruit} has already been imported', color='r')
        # else:
        query = f'''select * from ygb_quickbase_final_assigned_orders
         where `PURCHASE-DATE` >= "{date_to_recruit}" and `PURCHASE-DATE` < "{next_date_to_recruit}";
            '''
        print_color(query, color='y')
        df = pd.read_sql(query, con=engine)
        df.columns = [x.upper() for x in df.columns]
        df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
        df['ASIN'] = df['ASIN'].str.upper()
        df['SKU'] = df['SKU'].str.upper()

        df = df.merge(reference_df, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'], right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
        # df.to_csv(f'C:\\users\\ricky\\desktop\\data_sample.csv', index=False)
        print_color(df, color='p')

        data = []
        print_color(df.columns, color='y')

        for j in range(df.shape[0]):
            order_type = "Sale"
            record_id = str(df['RECORD_ID'].iloc[j])
            account_name = df['ACCOUNT_NAME'].iloc[j]
            quantity = str(df['QUANTITY'].iloc[j])
            purchase_date = df['PURCHASE-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
            item_price= df['ITEM-PRICE'].iloc[j]
            asin = df['ASIN'].iloc[j]
            amazon_order_id = df['AMAZON-ORDER-ID'].iloc[j]
            merchant_order_id = df['MERCHANT-ORDER-ID'].iloc[j]
            order_status = df['ORDER-STATUS'].iloc[j]
            fulfillment_channel = df['FULFILLMENT-CHANNEL'].iloc[j]
            sales_channel = df['SALES-CHANNEL'].iloc[j]
            order_channel = df['ORDER-CHANNEL'].iloc[j]
            ship_service_level = df['SHIP-SERVICE-LEVEL'].iloc[j]
            product_name = df['PRODUCT-NAME'].iloc[j]
            sku = df['SKU'].iloc[j]
            item_status = df['ITEM-STATUS'].iloc[j]
            currency = df['CURRENCY'].iloc[j]
            item_tax = str(df['ITEM-TAX'].iloc[j])
            shipping_price = str(df['SHIPPING-PRICE'].iloc[j])
            shipping_tax = str(df['SHIPPING-TAX'].iloc[j])
            gift_wrap_price = str(df['GIFT-WRAP-PRICE'].iloc[j])
            gift_wrap_tax = str(df['GIFT-WRAP-TAX'].iloc[j])
            item_promotion_discount = str(df['ITEM-PROMOTION-DISCOUNT'].iloc[j])
            ship_promotion_discount = str(df['SHIP-PROMOTION-DISCOUNT'].iloc[j])
            ship_city = df['SHIP-CITY'].iloc[j]
            ship_state = df['SHIP-STATE'].iloc[j]
            ship_postal_code = df['SHIP-POSTAL-CODE'].iloc[j]
            ship_country = df['SHIP-COUNTRY'].iloc[j]
            promotion_ids = df['PROMOTION-IDS'].iloc[j]
            is_business_order = df['IS-BUSINESS-ORDER'].iloc[j]
            purchase_order_number = df['PURCHASE-ORDER-NUMBER'].iloc[j]
            price_designation = df['PRICE-DESIGNATION'].iloc[j]
            is_transparency = df['IS-TRANSPARENCY'].iloc[j]
            signature_confirmation_recommended = df['SIGNATURE-CONFIRMATION-RECOMMENDED'].iloc[j]
            # status = df['STATUS'].iloc[j]
            fba_fee = str(df['FBA_FEE'].iloc[j])
            commission = str(df['COMMISSION'].iloc[j])
            principal = df['PRINCIPAL'].iloc[j]
            ranking = str(df['RANKING'].iloc[j])
            shipment_id = df['SHIPMENT-ID'].iloc[j]

            body = {
                x.upload_data.sales_fields.record_id: {"value": record_id},
                x.upload_data.sales_fields.account_name: {"value": account_name},
                x.upload_data.sales_fields.quantity: {"value": quantity},
                x.upload_data.sales_fields.purchase_date: {"value": purchase_date},
                x.upload_data.sales_fields.item_price: {"value": item_price},
                x.upload_data.sales_fields.asin: {"value": asin},
                x.upload_data.sales_fields.amazon_order_id: {"value": amazon_order_id},
                x.upload_data.sales_fields.merchant_order_id: {"value": merchant_order_id},
                x.upload_data.sales_fields.order_status: {"value": order_status},
                x.upload_data.sales_fields.fulfillment_channel: {"value": fulfillment_channel},
                x.upload_data.sales_fields.sales_channel: {"value": sales_channel},
                x.upload_data.sales_fields.order_channel: {"value": order_channel},
                x.upload_data.sales_fields.ship_service_level: {"value": ship_service_level},
                x.upload_data.sales_fields.product_name: {"value": product_name},
                x.upload_data.sales_fields.sku: {"value": sku},
                x.upload_data.sales_fields.item_status: {"value": item_status},
                x.upload_data.sales_fields.currency: {"value": currency},
                x.upload_data.sales_fields.item_tax: {"value": item_tax},
                x.upload_data.sales_fields.shipping_price: {"value": shipping_price},
                x.upload_data.sales_fields.shipping_tax: {"value": shipping_tax},
                x.upload_data.sales_fields.gift_wrap_price: {"value": gift_wrap_price},
                x.upload_data.sales_fields.gift_wrap_tax: {"value": gift_wrap_tax},
                x.upload_data.sales_fields.item_promotion_discount: {"value": item_promotion_discount},
                x.upload_data.sales_fields.ship_promotion_discount: {"value": ship_promotion_discount},
                x.upload_data.sales_fields.ship_city: {"value": ship_city},
                x.upload_data.sales_fields.ship_state: {"value": ship_state},
                x.upload_data.sales_fields.ship_postal_code: {"value": ship_postal_code},
                x.upload_data.sales_fields.ship_country: {"value": ship_country},
                x.upload_data.sales_fields.promotion_ids: {"value": promotion_ids},
                x.upload_data.sales_fields.is_business_order: {"value": is_business_order},
                x.upload_data.sales_fields.purchase_order_number: {"value": purchase_order_number},
                x.upload_data.sales_fields.price_designation: {"value": price_designation},
                x.upload_data.sales_fields.is_transparency: {"value": is_transparency},
                x.upload_data.sales_fields.signature_confirmation_recommended: {"value": signature_confirmation_recommended},
                x.upload_data.sales_fields.ranking: {"value": ranking},
                x.upload_data.sales_fields.shipment_id: {"value": shipment_id},
                x.upload_data.sales_fields.fba_fee: {"value": fba_fee},
                x.upload_data.sales_fields.commission: {"value": commission},
                x.upload_data.sales_fields.order_type: {"value": order_type}
            }
            data.append(body)
            # break
        print_color(data, color='g')
        if len(data) > 0:
            QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.sales_table_id,
                user_token=x.qb_user_token, apptoken=x.qb_app_token,username=x.username, password=x.password,
                filter_val=None, update_type='add_record', data=data, reference_column=None)

        counter +=1

        # break


def upload_returns_data(x, engine, start_date):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()

    QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
        table_id=x.sales_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
        username=x.qb_username, password=x.qb_password,
        filter_val= "Return",
        reference_column=x.upload_data.sales_fields.order_type,
        filter_type="EX"
    )

    df = pd.read_sql(f'''select * from ygb_quickbase_final_assigned_returns where `PURCHASE-DATE` >= "{start_date}";''',
                     con=engine)

    df.columns = [x.upper() for x in df.columns]
    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()
    df['SKU'] = df['SKU'].str.upper()

    print_color(df.shape[0], color='p')
    df = df.merge(reference_df, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'],
                  right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
    # df.to_csv(f'C:\\users\\ricky\\desktop\\data_sample.csv', index=False)
    print_color(df.shape[0], color='p')
    print_color(df.columns, color='y')

    counter = 0
    for i in range(0, df.shape[0], 1000):
        new_df = df.loc[i:i + 999]
        data = []
        for j in range(new_df.shape[0]):
            order_type = "Return"
            record_id = str(new_df['RECORD_ID'].iloc[j])
            account_name = new_df['ACCOUNT_NAME'].iloc[j]
            quantity = str(new_df['QUANTITY'].iloc[j])
            purchase_date = new_df['PURCHASE-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
            # item_price = new_df['ITEM-PRICE'].iloc[j]
            asin = new_df['ASIN'].iloc[j]
            amazon_order_id = new_df['ORDER-ID'].iloc[j]
            merchant_order_id = new_df['MERCHANT-ORDER-ID'].iloc[j]
            order_status = new_df['ORDER-STATUS'].iloc[j]
            product_name = new_df['PRODUCT-NAME'].iloc[j]
            sku = new_df['SKU'].iloc[j]
            item_status = new_df['ITEM-STATUS'].iloc[j]
            ranking = str(new_df['RANKING'].iloc[j])
            shipment_id = new_df['GROUP_ID'].iloc[j]
            fba_fee = new_df['FBA_FEE'].iloc[j]
            commission = new_df['COMMISSION'].iloc[j]
            item_price = new_df['PRINCIPAL'].iloc[j]

            body = {
                x.upload_data.sales_fields.record_id: {"value": record_id},
                x.upload_data.sales_fields.account_name: {"value": account_name},
                x.upload_data.sales_fields.quantity: {"value": quantity},
                x.upload_data.sales_fields.purchase_date: {"value": purchase_date},
                # x.upload_data.sales_fields.item_price: {"value": item_price},
                x.upload_data.sales_fields.asin: {"value": asin},
                x.upload_data.sales_fields.amazon_order_id: {"value": amazon_order_id},
                x.upload_data.sales_fields.merchant_order_id: {"value": merchant_order_id},
                x.upload_data.sales_fields.order_status: {"value": order_status},
                x.upload_data.sales_fields.product_name: {"value": product_name},
                x.upload_data.sales_fields.sku: {"value": sku},
                x.upload_data.sales_fields.item_status: {"value": item_status},
                x.upload_data.sales_fields.ranking: {"value": ranking},
                x.upload_data.sales_fields.shipment_id: {"value": shipment_id},
                x.upload_data.sales_fields.item_price: {"value": item_price},
                x.upload_data.sales_fields.fba_fee: {"value": fba_fee},
                x.upload_data.sales_fields.commission: {"value": commission},
                x.upload_data.sales_fields.order_type: {"value": order_type}

            }

            data.append(body)

        print_color(data, color='g')
        if len(data) > 0:
            QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.sales_table_id,
                user_token=x.qb_user_token,
                apptoken=x.qb_app_token,
                username=x.username,
                password=x.password,
                filter_val=None,
                update_type='add_record', data=data,
                reference_column=None
                )

        counter += 1
        # break


def upload_sales_fees_data(x, engine, start_date):
    Qb_API = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id)
    quickbase_product_data, product_column_dict = Qb_API.get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()

    Qb_API.purge_table_records(table_id=x.order_fees_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
                                username=x.qb_username, password=x.qb_password,
                                filter_val="Open",
                                reference_column=x.upload_data.order_fees_fields.status,
                            filter_type = "EX")

    imported_settlements = Qb_API.get_report_records(table_id=x.order_fees_table_id, report_id=5)
    if imported_settlements.shape[0] > 0:
        imported_settlements_ids = imported_settlements['Settlement_ID'].unique().tolist()
    else:
        imported_settlements_ids = []
    print_color(imported_settlements, color='b')

    settlement_ids = pd.read_sql(
        f'''select distinct  `SETTLEMENT-ID` from quickbase_settlement_order_data
            union
            select distinct  `SETTLEMENT-ID` from quickbase_finance_order_data
        ''', con=engine)['SETTLEMENT-ID'].unique().tolist()
    # print_color(settlement_ids, color='b')
    settlement_ids_to_import = [x for x in settlement_ids if str(x) not in imported_settlements_ids]
    print_color(settlement_ids_to_import, color='y')


    for each_settlement in settlement_ids_to_import:
        print_color(each_settlement, color='b')
        script = f'''
            select * from combined_quickbase_settlement_order_data where `SETTLEMENT-ID`  = "{each_settlement}"
             '''
        print_color(script, color='y')
        df  = pd.read_sql(script, con=engine)
        df.columns = [x.upper() for x in df.columns]
        df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
        df['ASIN'] = df['ASIN'].str.upper()
        df['SKU'] = df['SKU'].str.upper()
        print_color(df.shape[0], color='y')
        df = df.merge(reference_df, how='left',
                      left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'],
                      right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])

        print_color(df.shape[0], color='y')

        counter = 0
        for i in range(0, df.shape[0], 1000):
            print_color(i, i+999, color='r')
            new_df = df.loc[i:i + 999]
            # # print(new_df)
            qb_data = []
            for j in range(new_df.shape[0]):
                # record_id = str(df['RECORD_ID'].iloc[j])
                account_name = new_df['ACCOUNT_NAME'].iloc[j]
                settlement_id = str(new_df['SETTLEMENT-ID'].iloc[j])
                status = new_df['STATUS'].iloc[j]
                order_id = str(new_df['ORDER-ID'].iloc[j])
                sku = new_df['SKU'].iloc[j]
                start_date = new_df['START_DATE'].iloc[j].strftime('%Y-%m-%d')
                end_date = new_df['END_DATE'].iloc[j].strftime('%Y-%m-%d')
                posted_date =  new_df['POSTED-DATE'].iloc[j].strftime('%Y-%m-%d')
                fba_fee = str(new_df['FBA_FEE'].iloc[j]).replace("nan","0")
                commission = str(new_df['COMMISSION'].iloc[j]).replace("nan","0")
                asin = new_df['ASIN'].iloc[j]
                related_product = str(new_df['RECORD_ID'].iloc[j]).replace("nan","")
                transaction_type = new_df['TRANSACTION-TYPE'].iloc[j]
                ranking = str(new_df['RANKING'].iloc[j])
                group_id =str(new_df['GROUP_ID'].iloc[j]).replace("nan","")

                body = {

                    x.upload_data.order_fees_fields.account_name: {"value": account_name},
                    x.upload_data.order_fees_fields.settlement_id: {"value": settlement_id},
                    x.upload_data.order_fees_fields.status: {"value": status},
                    x.upload_data.order_fees_fields.order_id: {"value": order_id},
                    x.upload_data.order_fees_fields.sku: {"value": sku},
                    x.upload_data.order_fees_fields.fba_fee: {"value": fba_fee},
                    x.upload_data.order_fees_fields.commission: {"value": commission},
                    x.upload_data.order_fees_fields.asin: {"value": asin},
                    x.upload_data.order_fees_fields.related_product: {"value": related_product},

                    x.upload_data.order_fees_fields.start_date: {"value": start_date},
                    x.upload_data.order_fees_fields.end_date: {"value": end_date},
                    x.upload_data.order_fees_fields.posted_date: {"value": posted_date},

                    x.upload_data.order_fees_fields.transaction_type: {"value": transaction_type},
                    x.upload_data.order_fees_fields.group_id: {"value": group_id},
                    x.upload_data.order_fees_fields.ranking: {"value": ranking},

                }
                qb_data.append(body)
                # break
            # print_color(qb_data, color='r')
            print_color(f'Count of qb_data: {len(qb_data)}', color='g')

            print_color(qb_data, color='y')
            if len(qb_data) > 0:
                QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.order_fees_table_id,
                                                                                            user_token=x.qb_user_token,
                                                                                            apptoken=x.qb_app_token,
                                                                                            username=x.username,
                                                                                            password=x.password,
                                                                                            filter_val=None,
                                                                                            update_type='add_record',
                                                                                            data=qb_data,
                                                                                            reference_column=None)

                print_color(f'Batch {counter} Uploaded', color='G')
            counter += 1
            # break
        # break


def upload_finance_fees(x, engine):
    QBapi = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id)
    QBapi.purge_table_records(table_id=x.fees_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
                                username=x.qb_username, password=x.qb_password,
                                filter_val="Open",
                                reference_column=x.upload_data.settlement_fees_fields.status,
                            filter_type = "EX")


    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()

    df = pd.read_sql(f'Select * from quickbase_finances_upload', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()
    df['SKU'] = df['SKU'].str.upper()
    print_color(df, color='y')
    df = df.merge(reference_df, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'],
                      right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
    print_color(f'Dataframe Size {df.shape[0]}', color='b')
    # df = df[df['RECORD_ID'].isnull()]
    df['RECORD_ID'] = df['RECORD_ID'].apply(lambda x: "" if str(x) =="nan" else x)

    counter = 0
    for i in range(0, df.shape[0], 1000):
        print_color(f'Attempting to upload Batch {counter} {i}:{i + 999}', color='y')

        new_df = df.loc[i:i + 999]
        # print(new_df)
        qb_data = []
        for j in range(new_df.shape[0]):
            account_name = new_df['ACCOUNT_NAME'].iloc[j]
            settlement_id = str(new_df['SETTLEMENT-ID'].iloc[j])
            start_date = new_df['SETTLEMENT-START-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
            end_date = new_df['SETTLEMENT-END-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
            total_amount = str(new_df['TOTAL-AMOUNT'].iloc[j])
            # order_id = str(new_df['ORDER-ID'].iloc[j])
            transaction_type = new_df['TRANSACTION-TYPE'].iloc[j]
            fee_type = new_df['AMOUNT-TYPE'].iloc[j]
            fee_description = new_df['AMOUNT-DESCRIPTION'].iloc[j]
            posted_date = new_df['POSTED-DATE'].iloc[j].strftime('%Y-%m-%d')
            sku = new_df['SKU'].iloc[j]
            asin = new_df['ASIN'].iloc[j]
            amount = str(new_df['AMOUNT'].iloc[j])
            status = str(new_df['STATUS'].iloc[j])
            currency = str(new_df['CURRENCY'].iloc[j])

            related_product = str(new_df['RECORD_ID'].iloc[j])

            body = {
                x.upload_data.settlement_fees_fields.account_name: {"value": account_name},
                x.upload_data.settlement_fees_fields.settlement_id: {"value": settlement_id},
                x.upload_data.settlement_fees_fields.start_date: {"value": start_date},
                x.upload_data.settlement_fees_fields.end_date: {"value": end_date},
                x.upload_data.settlement_fees_fields.total_amount: {"value": total_amount},
                x.upload_data.settlement_fees_fields.status: {"value": status},
                x.upload_data.settlement_fees_fields.currency: {"value": currency},

                # x.upload_data.settlement_fees_fields.order_id: {"value": order_id},
                x.upload_data.settlement_fees_fields.transaction_type: {"value": transaction_type},
                x.upload_data.settlement_fees_fields.fee_type: {"value": fee_type},
                x.upload_data.settlement_fees_fields.fee_description: {"value": fee_description},
                x.upload_data.settlement_fees_fields.date: {"value": posted_date},
                x.upload_data.settlement_fees_fields.sku: {"value": sku},
                x.upload_data.settlement_fees_fields.asin: {"value": asin},
                x.upload_data.settlement_fees_fields.amount: {"value": amount},
                x.upload_data.settlement_fees_fields.related_product: {"value": related_product},

            }
            qb_data.append(body)

        print_color(f'Count of qb_data: {len(qb_data)}', color='g')

        if len(qb_data) > 0:
            QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.fees_table_id,
                                                                                        user_token=x.qb_user_token,
                                                                                        apptoken=x.qb_app_token,
                                                                                        username=x.username,
                                                                                        password=x.password,
                                                                                        filter_val=None,
                                                                                        update_type='add_record',
                                                                                        data=qb_data,
                                                                                        reference_column=None)

            print_color(f'Batch {counter} Uploaded', color='G')
        counter += 1
        # break


def upload_settlement_fees(x, engine, start_date):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()

    # reference_df.to_csv(f'C:\\users\\{getpass.getuser()}\\desktop\\product_data.csv', index=False)

    scripts = []
    scripts.append(f'drop table if exists ygb_product_account_asin;')
    scripts.append(f'''create table if not exists ygb_product_account_asin(primary key(account_name, sku))
        select * from
        (select account_name, sku, asin, VARIATIONTYPE, 
        row_number() over (partition by account_name, sku order by sku, case when  VARIATIONTYPE = "child" then 1 else 2 end) as ranking 
        from product_data) A
        where ranking = 1''')

    run_sql_scripts(engine=engine, scripts=scripts)

    imported_settlements = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_report_records(
        table_id=x.fees_table_id, report_id=11)
    if imported_settlements.shape[0] >0:
        imported_settlements_ids = imported_settlements['Settlement_ID'].unique().tolist()
    else:
        imported_settlements_ids = []
    print_color(imported_settlements, color='b')


    settlement_ids = pd.read_sql(f'''Select distinct `SETTLEMENT-ID` from settlements
            where `SETTLEMENT-START-DATE` >= "{start_date}"''', con=engine)['SETTLEMENT-ID'].unique().tolist()

    settlement_ids_to_import = [x for x in settlement_ids if str(x) not in imported_settlements_ids]
    print_color(imported_settlements_ids, color='b')
    print_color(settlement_ids_to_import, color='b')

    for each_settlement in settlement_ids_to_import:
        df = pd.read_sql(f'''select "Posted" as STATUS, ACCOUNT_NAME, `SETTLEMENT-ID`, CURRENCY,
            `SETTLEMENT-START-DATE`, `SETTLEMENT-END-DATE`,
            `TOTAL-AMOUNT`, `TRANSACTION-TYPE`,
            `AMOUNT-TYPE`, `AMOUNT-DESCRIPTION`, `POSTED-DATE`, `SKU`,asin, sum(`AMOUNT`)  as AMOUNT from settlements A
            left join ygb_product_account_asin B using(account_name, sku)
           --  where `SETTLEMENT-START-DATE` >= "{start_date}"
            where `SETTLEMENT-ID` = "{each_settlement}"
            group by
            ACCOUNT_NAME, `SETTLEMENT-ID`,
            `SETTLEMENT-START-DATE`, `SETTLEMENT-END-DATE`,
            `TOTAL-AMOUNT`,`TRANSACTION-TYPE`,
            `AMOUNT-TYPE`, `AMOUNT-DESCRIPTION`, `POSTED-DATE`, `SKU`,asin;''', con=engine)
        df.columns = [x.upper() for x in df.columns]

        df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
        df['ASIN'] = df['ASIN'].str.upper()
        df['SKU'] = df['SKU'].str.upper()

        print_color(f'Dataframe Size {df.shape[0]}', color='b')
        df = df.merge(reference_df, how='left',
              left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'],
              right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
        print_color(f'Dataframe Size {df.shape[0]}', color='b')
        # df = df[df['RECORD_ID'].isnull()]
        df['RECORD_ID'] = df['RECORD_ID'].apply(lambda x: "" if str(x) =="nan" else x)
        print_color(df, color='y')

        counter = 0
        for i in range(0, df.shape[0], 1000):
            print_color(f'Attempting to upload Batch {counter} {i}:{i+999}',color='y')

            new_df = df.loc[i:i+999]
            # print(new_df)
            qb_data = []
            for j in range(new_df.shape[0]):


                account_name = new_df['ACCOUNT_NAME'].iloc[j]
                settlement_id = str(new_df['SETTLEMENT-ID'].iloc[j])
                start_date =  new_df['SETTLEMENT-START-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
                end_date  = new_df['SETTLEMENT-END-DATE'].iloc[j].strftime('%Y-%m-%dT%H:%M:%S')
                total_amount = str(new_df['TOTAL-AMOUNT'].iloc[j])
                # order_id = str(new_df['ORDER-ID'].iloc[j])
                transaction_type = new_df['TRANSACTION-TYPE'].iloc[j]
                fee_type = new_df['AMOUNT-TYPE'].iloc[j]
                fee_description = new_df['AMOUNT-DESCRIPTION'].iloc[j]
                posted_date = new_df['POSTED-DATE'].iloc[j].strftime('%Y-%m-%d')
                sku = new_df['SKU'].iloc[j]
                asin = new_df['ASIN'].iloc[j]
                amount = str(new_df['AMOUNT'].iloc[j])
                status = str(new_df['STATUS'].iloc[j])
                currency = str(new_df['CURRENCY'].iloc[j])

                related_product = str(new_df['RECORD_ID'].iloc[j])

                body = {
                    x.upload_data.settlement_fees_fields.account_name: {"value": account_name},
                    x.upload_data.settlement_fees_fields.settlement_id: {"value": settlement_id},
                    x.upload_data.settlement_fees_fields.start_date: {"value": start_date},
                    x.upload_data.settlement_fees_fields.end_date: {"value": end_date},
                    x.upload_data.settlement_fees_fields.total_amount: {"value": total_amount},
                    x.upload_data.settlement_fees_fields.status: {"value": status},
                    x.upload_data.settlement_fees_fields.currency: {"value": currency},
                    # x.upload_data.settlement_fees_fields.order_id: {"value": order_id},
                    x.upload_data.settlement_fees_fields.transaction_type: {"value": transaction_type},
                    x.upload_data.settlement_fees_fields.fee_type: {"value": fee_type},
                    x.upload_data.settlement_fees_fields.fee_description: {"value": fee_description},
                    x.upload_data.settlement_fees_fields.date: {"value": posted_date},
                    x.upload_data.settlement_fees_fields.sku: {"value": sku},
                    x.upload_data.settlement_fees_fields.asin: {"value": asin},
                    x.upload_data.settlement_fees_fields.amount: {"value": amount},
                    x.upload_data.settlement_fees_fields.related_product: {"value": related_product},

                }
                qb_data.append(body)
                # break
            print_color(f'Count of qb_data: {len(qb_data)}', color='g')
            if len(qb_data) > 0:
                QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.fees_table_id,
                    user_token=x.qb_user_token, apptoken=x.qb_app_token,username=x.username, password=x.password,
                    filter_val=None, update_type='add_record', data=qb_data, reference_column=None)

                print_color(f'Batch {counter} Uploaded', color='G')
            counter += 1

        # break


def upload_shipment_data(x, engine):
    filter_list = ['RECEIVING', 'DELIVERED', 'WORKING', 'READY_TO_SHIP', 'SHIPPED',  'ERROR', 'IN_TRANSIT', 'CHECKED_IN']
    for each_filter in filter_list:
        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.shipment_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val=each_filter,
            reference_column=x.upload_data.shipment_fields.shipment_status
        )

    quickbase_shipment_data, shipment_column_dict = QuickbaseAPI(hostname=x.qb_hostname,
         auth=x.qb_auth,app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.shipment_table_id,
        col_order=x.upload_data.shipment_fields.col_order,
        filter=x.upload_data.shipment_fields.filter,
        field_id=x.upload_data.shipment_fields.field_id,
        filter_type=x.upload_data.shipment_fields.filter_type,
        filter_operator=x.upload_data.shipment_fields.filter_operator
    )

    print_color(quickbase_shipment_data, color='y')
    account_name_column = shipment_column_dict.get(x.upload_data.shipment_fields.account_name)
    shipment_column = shipment_column_dict.get(x.upload_data.shipment_fields.shipment_id)


    print_color(quickbase_shipment_data, color='r')

    df = pd.read_sql(f'''select ACCOUNT_NAME, SHIPMENTID, SHIPMENTNAME, NAME,
          ADDRESSLINE1, city, STATEORPROVINCECODE, COUNTRYCODE, POSTALCODE,
          DESTINATIONFULFILLMENTCENTERID, SHIPMENTSTATUS
          from inbound_shipments''', con=engine)
    df.columns = [x.upper() for x in df.columns]
    print_color(df, color='y')
    df.insert(0, "Source", "SQL")

    if quickbase_shipment_data.shape[0]>0:
        reference_df = quickbase_shipment_data[[account_name_column, shipment_column]]
        reference_df.columns = ['ACCOUNT_NAME', 'SHIPMENTID']
        reference_df.insert(0, "Source", "Quickbase")


        print_color(reference_df, color='y')
        new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SHIPMENTID'],
                                                                           keep=False)
        new_df = new_df[new_df['Source'] == 'SQL']

    else:
        new_df = df






    print_color(new_df, color='g')
    data = []



    for i in range(new_df.shape[0]):
        account_name = new_df['ACCOUNT_NAME'].iloc[i]
        shipment_id = new_df['SHIPMENTID'].iloc[i]
        shipment_name = new_df['SHIPMENTNAME'].iloc[i]
        name = new_df['NAME'].iloc[i]
        address = new_df['ADDRESSLINE1'].iloc[i]
        city = new_df['CITY'].iloc[i]
        state = new_df['STATEORPROVINCECODE'].iloc[i]
        country = new_df['COUNTRYCODE'].iloc[i]
        postal_code = str(new_df['POSTALCODE'].iloc[i])
        fulfillment_center = new_df['DESTINATIONFULFILLMENTCENTERID'].iloc[i]
        shipment_status = new_df['SHIPMENTSTATUS'].iloc[i]

        body = {
            x.upload_data.shipment_fields.account_name: {"value": account_name},
            x.upload_data.shipment_fields.shipment_id: {"value": shipment_id},
            x.upload_data.shipment_fields.shipment_name: {"value": shipment_name},
            x.upload_data.shipment_fields.name: {"value": name},
            x.upload_data.shipment_fields.address: {"value": address},
            x.upload_data.shipment_fields.city: {"value": city},
            x.upload_data.shipment_fields.state: {"value": state},
            x.upload_data.shipment_fields.country: {"value": country},
            x.upload_data.shipment_fields.postal_code: {"value": postal_code},
            x.upload_data.shipment_fields.fulfillment_center: {"value": fulfillment_center},
            x.upload_data.shipment_fields.shipment_status: {"value": shipment_status}
        }
        data.append(body)
        # break

    print_color(data, color='b')
    if len(data) > 0:
        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(
            table_id=x.shipment_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.username, password=x.password, filter_val=None, update_type='add_record',
            data=data, reference_column=None)


def upload_shipment_detail_data(x, engine):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    product_reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    product_reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(product_reference_df, color='r')
    product_reference_df['SKU'] = product_reference_df['SKU'].str.upper()
    product_reference_df['ASIN'] = product_reference_df['ASIN'].str.upper()


    filter_list = ['RECEIVING', 'DELIVERED', 'WORKING', 'READY_TO_SHIP', 'SHIPPED', 'ERROR', 'IN_TRANSIT', 'CHECKED_IN']
    for each_filter in filter_list:
        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.shipment_detail_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val=each_filter,
            reference_column=x.upload_data.shipment_detail_fields.shipment_status
        )
        # break

    quickbase_shipment_data, shipment_column_dict = QuickbaseAPI(hostname=x.qb_hostname,
                                                                 auth=x.qb_auth,
                                                                 app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.shipment_detail_table_id,
        col_order=x.upload_data.shipment_detail_fields.col_order,
        filter=x.upload_data.shipment_detail_fields.filter,
        field_id=x.upload_data.shipment_detail_fields.field_id,
        filter_type=x.upload_data.shipment_detail_fields.filter_type,
        filter_operator=x.upload_data.shipment_detail_fields.filter_operator
    )

    print_color(quickbase_shipment_data, color='y')
    if quickbase_shipment_data.shape[0]>0:
        account_name_column = shipment_column_dict.get(x.upload_data.shipment_detail_fields.account_name)
        shipment_column = shipment_column_dict.get(x.upload_data.shipment_detail_fields.shipment_id)
        sku_column = shipment_column_dict.get(x.upload_data.shipment_detail_fields.sku)
        fnsku_column = shipment_column_dict.get(x.upload_data.shipment_detail_fields.fnsku)

        reference_df = quickbase_shipment_data[[account_name_column, shipment_column,sku_column, fnsku_column]]
        reference_df.columns = ['ACCOUNT_NAME', 'SHIPMENTID', 'SKU', 'FNSKU']

    else:
        reference_df = pd.DataFrame()
    # print_color(reference_df, color='y')
    df = pd.read_sql(f'''SELECT a.ACCOUNT_NAME, SHIPMENTID, SELLERSKU as SKU, FULFILLMENTNETWORKSKU as FNSKU, 
        CASE WHEN B.ASIN IS NOT NULL THEN B.ASIN 
            WHEN a.FULFILLMENTNETWORKSKU LIKE 'B%' THEN  a.FULFILLMENTNETWORKSKU
            ELSE NULL END AS ASIN, QUANTITYSHIPPED,
        QUANTITYRECEIVED, QUANTITYINCASE, RELEASEDATE
        FROM inbound_shipments_detail A left join quickbase_product_data B ON A.account_name = B.account_name AND A.SELLERSKU = B.SKU;
                ''', con=engine)


    df.columns = [x.upper() for x in df.columns]
    print_color(df, color='y')

    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    df['SKU'] = df['SKU'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()
    new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SHIPMENTID', 'SKU', 'FNSKU' ],
                                                                 keep=False)


    new_df = new_df.merge(product_reference_df, how='left',
          left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'], right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
    print_color(new_df, color='g')

    # product_reference_df.to_csv(f'C:\\users\\Ricky\\desktop\\product_data_export.csv', index=False)
    # new_df.to_csv(f'C:\\users\\Ricky\\desktop\\shipment_detail_export.csv', index=False)
    data = []

    print( new_df['RECORD_ID'].unique())

    new_df['RECORD_ID'] = new_df['RECORD_ID'].apply(lambda x: int(x) if str(x) != "nan" else "")
    new_df['RECORD_ID'] = new_df['RECORD_ID'].astype(str)

    print_color(new_df, color='y')
    print(new_df['RECORD_ID'].unique())
    for i in range(new_df.shape[0]):
        related_product = str(new_df['RECORD_ID'].iloc[i])
        account_name = new_df['ACCOUNT_NAME'].iloc[i]
        shipment_id = new_df['SHIPMENTID'].iloc[i]
        sku = new_df['SKU'].iloc[i]
        asin = new_df['ASIN'].iloc[i]
        shipped_qty = str(new_df['QUANTITYSHIPPED'].iloc[i])
        received_qty = str(new_df['QUANTITYRECEIVED'].iloc[i])
        qty_in_case = str(new_df['QUANTITYINCASE'].iloc[i])
        release_date = new_df['RELEASEDATE'].iloc[i]
        fnsku = new_df['FNSKU'].iloc[i]
        print(release_date)
        release_date = release_date.strftime('%Y-%m-%d') if release_date is not None else release_date

        body = {
            x.upload_data.shipment_detail_fields.account_name: {"value": account_name},
            x.upload_data.shipment_detail_fields.shipment_id: {"value": shipment_id},
            x.upload_data.shipment_detail_fields.sku: {"value": sku},
            x.upload_data.shipment_detail_fields.asin: {"value": asin},
            x.upload_data.shipment_detail_fields.related_product: {"value": related_product},

            x.upload_data.shipment_detail_fields.shipped_qty: {"value": shipped_qty},
            x.upload_data.shipment_detail_fields.received_qty: {"value": received_qty},
            x.upload_data.shipment_detail_fields.qty_in_case: {"value": qty_in_case},
            x.upload_data.shipment_detail_fields.release_date: {"value": release_date},
            x.upload_data.shipment_detail_fields.fnsku: {"value": fnsku}
        }
        data.append(body)
        # break

    print_color(data, color='b')
    if len(data) > 0:
        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(
            table_id=x.shipment_detail_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.username, password=x.password, filter_val=None, update_type='add_record',
            data=data, reference_column=None)


def upload_shipment_tracking(x, engine):
    filter_list = ['RECEIVING', 'DELIVERED', 'WORKING', 'READY_TO_SHIP', 'SHIPPED', 'ERROR', 'IN_TRANSIT', 'CHECKED_IN']
    for each_filter in filter_list:
        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.shipment_tracking_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val=each_filter,
            reference_column=x.upload_data.shipment_tracking_fields.shipment_status
        )
        # break

    quickbase_tracking_data, tracking_column_dict = QuickbaseAPI(hostname=x.qb_hostname,
                                                                 auth=x.qb_auth,
                                                                 app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.shipment_tracking_table_id,
        col_order=x.upload_data.shipment_tracking_fields.col_order,
        filter=x.upload_data.shipment_tracking_fields.filter,
        field_id=x.upload_data.shipment_tracking_fields.field_id,
        filter_type=x.upload_data.shipment_tracking_fields.filter_type,
        filter_operator=x.upload_data.shipment_tracking_fields.filter_operator
    )

    print_color(quickbase_tracking_data, color='y')
    account_name_column = tracking_column_dict.get(x.upload_data.shipment_tracking_fields.account_name)
    shipment_column = tracking_column_dict.get(x.upload_data.shipment_tracking_fields.shipment_id)

    df = pd.read_sql(f'''select ACCOUNT_NAME, SHIPMENTID,
            SHIPMENTTYPE, TRACKINGID, TRANSPORTSTATUS, CARRIERNAME, VALUE as COST, CURRENCYCODE as CURRENCY, 
            WEIGHT_VALUE, WEIGHT_UNIT, PACKAGESTATUS
            from inbound_transport_detail''', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df = df.replace(np.nan, "")

    if quickbase_tracking_data.shape[0]>0:
        reference_df = quickbase_tracking_data[[account_name_column, shipment_column]]
        reference_df.columns = ['ACCOUNT_NAME', 'SHIPMENTID']
        new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SHIPMENTID'], keep=False)
    else:
        new_df = df
    # new_df = df
    print_color(new_df, color='g')
    new_df = new_df.replace(np.nan, "")
    data = []

    for i in range(new_df.shape[0]):
        account_name = new_df['ACCOUNT_NAME'].iloc[i]
        shipment_id = new_df['SHIPMENTID'].iloc[i]
        shipment_type = new_df['SHIPMENTTYPE'].iloc[i]
        tracking_id = new_df['TRACKINGID'].iloc[i]
        transport_status = str(new_df['TRANSPORTSTATUS'].iloc[i])
        carrier = str(new_df['CARRIERNAME'].iloc[i])
        cost = new_df['COST'].iloc[i]

        currency = new_df['CURRENCY'].iloc[i]
        weight_value = new_df['WEIGHT_VALUE'].iloc[i]
        weight_unit = new_df['WEIGHT_UNIT'].iloc[i]
        package_status = new_df['PACKAGESTATUS'].iloc[i]


        body = {
            x.upload_data.shipment_tracking_fields.account_name: {"value": account_name},
            x.upload_data.shipment_tracking_fields.shipment_id: {"value": shipment_id},
            x.upload_data.shipment_tracking_fields.shipment_type: {"value": shipment_type},
            x.upload_data.shipment_tracking_fields.tracking_id: {"value": tracking_id},
            x.upload_data.shipment_tracking_fields.transport_status: {"value": transport_status},
            x.upload_data.shipment_tracking_fields.carrier: {"value": carrier},
            x.upload_data.shipment_tracking_fields.cost: {"value": cost},
            x.upload_data.shipment_tracking_fields.currency: {"value": currency},
            x.upload_data.shipment_tracking_fields.weight_value: {"value": weight_value},
            x.upload_data.shipment_tracking_fields.weight_unit: {"value": weight_unit},
            x.upload_data.shipment_tracking_fields.package_status: {"value": package_status}
        }
        data.append(body)
        # break
    #
    print_color(data, color='b')
    if len(data) > 0:
        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(
            table_id=x.shipment_tracking_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.username, password=x.password, filter_val=None, update_type='add_record',
            data=data, reference_column=None)


def upload_inventory_data(x, engine):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()


    df = pd.read_sql(f'''select * from quickbase_inventory''', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df = df.replace(np.nan, "")
    # new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SHIPMENTID'],
    #                                                                    keep=False)

    df.columns = [x.upper() for x in df.columns]
    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()
    df['SKU'] = df['SKU'].str.upper()

    df = df.merge(reference_df, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'], right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])

    new_df = df
    print_color(new_df, color='g')
    # new_df.to_csv(f'C:\\users\\Ricky\\desktop\\inventory_export.csv', index=False)
    data = []

    for i in range(new_df.shape[0]):
        record_id = str(new_df['RECORD_ID'].iloc[i])
        account_name = new_df['ACCOUNT_NAME'].iloc[i]
        sku = str(new_df['SKU'].iloc[i])
        asin = new_df['ASIN'].iloc[i]
        condition = new_df['CONDITION'].iloc[i]
        price = str(new_df['PRICE'].iloc[i])
        mfn_listing_exists = new_df['MFN_LISTING_EXISTS'].iloc[i]
        afn_listing_exists = new_df['AFN_LISTING_EXISTS'].iloc[i]
        mfn_fulfillable_qty = str(new_df['MFN_FULFILLBALE_QTY'].iloc[i])
        afn_warehouse_qty = str(new_df['AFN_WAREHOUSE_QTY'].iloc[i])
        afn_fulfillable_qty = str(new_df['AFN_FULFILLABLE_QTY'].iloc[i])
        afn_unsellable_qty = str(new_df['AFB_UNSELLABLE_QTY'].iloc[i])
        afn_unsellable_defective = str(new_df['DEFECTIVE'].iloc[i])
        afn_unsellable_warehouse_damaged = str(new_df['WAREHOUSE_DAMAGED'].iloc[i])
        afn_unsellable_customer_damaged = str(new_df['CUSTOMER_DAMAGED'].iloc[i])
        afn_inbound_qty = str(new_df['AFN_INBOUND_QTY'].iloc[i])
        afn_inbound_working = str(new_df['AFN_INBOUND_WORKING'].iloc[i])
        afn_inbound_shipped = str(new_df['AFN_INBOUND_SHIPPED'].iloc[i])
        afn_inbound_receiving = str(new_df['AFN_INBOUND_RECEIVING'].iloc[i])
        afn_researching_qty = str(new_df['AFN_RESEARCHING_QTY'].iloc[i])
        afn_reserved_qty = str(new_df['AFN_RESERVED_QTY'].iloc[i])
        afn_reserved_customer_orders = str(new_df['AFN_RESERVED_CUSTOMER_ORDERS'].iloc[i])
        afn_reserved_fc_transfers = str(new_df['AFN_RESERVED_FC_TRANSFERS'].iloc[i])
        afn_reserved_fc_processing = str(new_df['AFN_RESERVED_FC_PROCESSING'].iloc[i])
        afn_total_qty = str(new_df['AFN_TOTAL_QTY'].iloc[i])

        body = {
            x.upload_data.fba_inventory_fields.related_product: {"value": record_id},
            x.upload_data.fba_inventory_fields.condition: {"value": condition},
            x.upload_data.fba_inventory_fields.price: {"value": price},
            x.upload_data.fba_inventory_fields.mfn_listing_exists: {"value": mfn_listing_exists},
            x.upload_data.fba_inventory_fields.afn_listing_exists: {"value": afn_listing_exists},
            x.upload_data.fba_inventory_fields.mfn_fulfillable_qty: {"value": mfn_fulfillable_qty},
            x.upload_data.fba_inventory_fields.afn_warehouse_qty: {"value": afn_warehouse_qty},
            x.upload_data.fba_inventory_fields.afn_fulfillable_qty: {"value": afn_fulfillable_qty},
            x.upload_data.fba_inventory_fields.afn_unsellable_qty: {"value": afn_unsellable_qty},
            x.upload_data.fba_inventory_fields.afn_unsellable_defective: {"value": afn_unsellable_defective},
            x.upload_data.fba_inventory_fields.afn_unsellable_warehouse_damaged: {"value": afn_unsellable_warehouse_damaged},
            x.upload_data.fba_inventory_fields.afn_unsellable_customer_damaged: {"value": afn_unsellable_customer_damaged},
            x.upload_data.fba_inventory_fields.afn_inbound_qty: {"value": afn_inbound_qty},
            x.upload_data.fba_inventory_fields.afn_inbound_working: {"value": afn_inbound_working},
            x.upload_data.fba_inventory_fields.afn_inbound_shipped: {"value": afn_inbound_shipped},
            x.upload_data.fba_inventory_fields.afn_inbound_receiving: {"value": afn_inbound_receiving},
            x.upload_data.fba_inventory_fields.afn_researching_qty: {"value": afn_researching_qty},
            x.upload_data.fba_inventory_fields.afn_reserved_qty: {"value": afn_reserved_qty},
            x.upload_data.fba_inventory_fields.afn_reserved_customer_orders: {"value": afn_reserved_customer_orders},
            x.upload_data.fba_inventory_fields.afn_reserved_fc_transfers: {"value": afn_reserved_fc_transfers},
            x.upload_data.fba_inventory_fields.afn_reserved_fc_processing: {"value": afn_reserved_fc_processing},
            x.upload_data.fba_inventory_fields.account_name: {"value": account_name},
            x.upload_data.fba_inventory_fields.sku: {"value": sku},
            x.upload_data.fba_inventory_fields.asin: {"value": asin},
            x.upload_data.fba_inventory_fields.afn_total_qty: {"value": afn_total_qty}
        }
        data.append(body)
        # break
    #
    print_color(data, color='b')
    if len(data) > 0:
        each_filter = account_name
        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.fba_inventory_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val=each_filter,
            reference_column=x.upload_data.fba_inventory_fields.account_name
        )

        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(
            table_id=x.fba_inventory_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.username, password=x.password, filter_val=None, update_type='add_record',
            data=data, reference_column=None)


def upload_factory_pos(x, engine, start_date):
    quickbase_product_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    reference_df = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    reference_df.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(reference_df, color='r')
    reference_df['SKU'] = reference_df['SKU'].str.upper()
    reference_df['ACCOUNT_NAME'] = reference_df['ACCOUNT_NAME'].str.upper()


    client_secret_file = f'{x.project_folder}\\Text Files\\client_secret.json'
    token_file = f'{x.project_folder}\\Text Files\\token.json'
    sheet_id = '1bFIz-jt-LA7_NaU8EbJCM_HzIFUs0DfAM69foOxQ-Ic'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


    columns = ['Shipped With', 'Location Status', 'Shippin Plan', 'Invoice #', 'Invoice Date', 'Factory Name', 'Contact Person',
     'SKU', 'QTY', 'Order Placed', 'Completion Date', 'Unit Price', 'Invoice Amount', 'Deposit Sent', 'Confirmation #',
     'Date Paid', 'Balance Owed', 'Confirmation # 1', 'Date Paid 1', 'Production Status', 'Shipping to', 'Shipping to Location',
     'FBA Shipment ID', 'Container #', 'Deliver to', 'Trucking by', 'Master Bill of Lading #', 'Total Cartons', 'CBM',
     'ETD', 'ETA', 'Status', 'Remarks', 'Payment Sent', 'AN Received', 'Customs Released / Sent docs', 'Telex Released',
               'DO sent',
     'Pick Up Scheduled', 'Delivery Scheduled']

    sheetname = 'Completed Shipments'
    GsheetAPI = GoogleSheetsAPI(client_secret_file,token_file,SCOPES,  sheet_id )
    completed_df = GsheetAPI. get_data_from_sheet(sheetname=sheetname, range_name='A:AG')
    for i in range(len(columns) - len(completed_df.columns)):
        completed_df[i] = None

    completed_df.columns = columns
    print_color(completed_df, color='r')


    completed_df.insert(0, "PO Type", "Completed")

    print_color(completed_df.columns, color='r')

    sheetname = 'POs'
    po_df = GsheetAPI.get_data_from_sheet(sheetname=sheetname, range_name='A:AN')
    po_df.columns = columns
    print_color(po_df, color='r')
    po_df = po_df.replace("FALSE", None)
    po_df.insert(0, "PO Type", "Open POs")

    new_df = pd.concat([completed_df, po_df], sort=False)
    new_df.columns = [x.upper() for x in new_df.columns]

    # new_df.to_csv(f'C:\\users\\{getpass.getuser()}\\desktop\\po_data.csv', index=False)

    new_df = new_df.dropna(subset=['QTY'])
    new_df = new_df[(new_df['QTY'] != "")]
    # print_color(new_df['QTY'].unique(), color='g')
    # print_color(new_df, color='b')


    new_df['ETA'] = new_df['ETA'].str.replace(".","/")
    new_df['ETD'] = new_df['ETD'].str.replace(".", "/")
    new_df['ORDER PLACED'] = new_df['ORDER PLACED'].str.replace(".", "/")
    new_df['DATE PAID'] = new_df['DATE PAID'].str.replace(".", "/")
    new_df['COMPLETION DATE'] = new_df['COMPLETION DATE'].str.replace(".", "/").str.replace("Ready","").str.replace("; no confirmation","").str.replace("  "," ").str.strip()

    # new_df['ETA'] = new_df['ETA'].apply(lambda x: x if isinstance(x, datetime.datetime) else x)
    # new_df['ETA'] = pd.to_datetime(new_df['ETA'])
    print_color(new_df['ETD'].unique(), color='p')
    new_df['ETD'] = new_df['ETD'].fillna(value="")
    new_df['ETD'] = new_df['ETD'].apply(lambda x: parse(x, fuzzy=False) if is_date(string=x, fuzzy=False) else "" )
    new_df['ETD'] = pd.to_datetime(new_df['ETD'])

    new_df['ETA'] = new_df['ETA'].fillna(value="")
    new_df['ETA'] = new_df['ETA'].apply(lambda x: parse(x, fuzzy=False) if is_date(string=x, fuzzy=False) else "")
    new_df['ETA'] = pd.to_datetime(new_df['ETA'])


    new_df['ORDER PLACED'] = pd.to_datetime(new_df['ORDER PLACED'])
    new_df['DATE PAID'] = pd.to_datetime(new_df['DATE PAID'])
    new_df['READY'] = new_df['COMPLETION DATE'].apply(lambda x: True if x.isalpha() else False)
    print_color( new_df['ETD'].unique(), color='p')


    print_color(new_df.shape[0], color='p')
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    new_df = new_df[(new_df['ETA'] >= f"{start_date}") | (new_df['PO TYPE'] == "Open POs")]
    print_color(new_df.shape[0], color='p')

    # new_df.to_csv(f'C:\\users\\{getpass.getuser()}\\desktop\\completed_pos.csv', index=False)

    for i in range(new_df.shape[0]):
        value = new_df['COMPLETION DATE'].iloc[i]
        if any(c.isalpha() for c in value):
            new_df['COMPLETION DATE'].iloc[i] = ""
        else:
            new_df['COMPLETION DATE'].iloc[i] = value
            # if
            print_color(value, color='y')
        # print(value, re.search('[a-zA-Z]', value))
    new_df.to_csv(f'C:\\users\\{getpass.getuser()}\\desktop\\ygb po data.csv', index=False)
    # print_color(new_df['COMPLETION DATE'].unique(), color='p')
    new_df['COMPLETION DATE'] = pd.to_datetime(new_df['COMPLETION DATE'], errors = 'coerce')


    new_df.insert(0, "ACCOUNT_NAME", "YGB GROUP")
    new_df['SKU'] = new_df['SKU'].str.upper()
    new_df['ACCOUNT_NAME'] = new_df['ACCOUNT_NAME'].str.upper()

    new_df = new_df.merge(reference_df, how='left', left_on=['ACCOUNT_NAME', 'SKU',], right_on=['ACCOUNT_NAME', 'SKU'])
    print_color(new_df.columns, color='r')
    new_df['RECORD_ID'] = new_df['RECORD_ID'].replace(np.nan, "")
    new_df['ASIN'] = new_df['ASIN'].replace(np.nan, "")

    for col in list(new_df.columns):
        new_df[col] = new_df[col].apply(lambda x: "" if x is None else x)

    for col in ['UNIT PRICE', 'INVOICE AMOUNT', 'DEPOSIT SENT', 'BALANCE OWED', 'QTY']:
        new_df[col] =  new_df[col].str.replace("$","").str.replace(",","")

    # print_color(new_df.shape[0], color='b')

    sheet_id = '1zn0oOjftd3bJxCNh2T2gGQGZrJEy4jYJDSgCkDtLO3I'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    columns = []

    sheetname = 'COGs'
    cogs_df = GoogleSheetsAPI(client_secret_file, token_file, SCOPES, sheet_id).get_data_from_sheet(
        sheetname=sheetname, range_name='A:W')
    cogs_df.columns = [x.upper() for x in  cogs_df.columns]
    print_color(cogs_df, color='r')

    # for i in range(len(columns) - len(completed_df.columns)):
    #     completed_df[i] = None
    #
    # completed_df.columns = columns
    # print_color(completed_df, color='r')

    cogs_df = cogs_df[['SKU', 'CONTAINER_#','DUTIES','CUSTOMS_FEES' , 'DEMURRAGE' , 'CONTAINER_COST', 'TRUCKING_COST']]
    # cogs_df = cogs_df.str.replace("$", "")
    cogs_df['SKU'] = cogs_df['SKU'].apply(lambda x: x.strip() if x is not None else x)
    cogs_df['CONTAINER_#'] = cogs_df['CONTAINER_#'].apply(lambda x: x.strip() if x is not None else x)

    new_df = new_df.merge(cogs_df, how='left', left_on=['SKU', 'CONTAINER #'], right_on=['SKU', 'CONTAINER_#'])
    new_df['DUTIES'] = new_df['DUTIES'].str.replace("$", "").str.replace(",", "").fillna(0)
    new_df['CUSTOMS_FEES'] = new_df['CUSTOMS_FEES'].str.replace("$", "").str.replace(",", "").fillna(0)
    new_df['DEMURRAGE'] = new_df['DEMURRAGE'].str.replace("$", "").str.replace(",", "").fillna(0)
    new_df['CONTAINER_COST'] = new_df['CONTAINER_COST'].str.replace("$", "").str.replace(",", "").fillna(0)
    new_df['TRUCKING_COST'] = new_df['TRUCKING_COST'].str.replace("$", "").str.replace(",", "").fillna(0)

    print_color(new_df, color='y')

    # new_df['REMARKS'] = new_df['REMARKS'].fillna("",inplace=True)
    # new_df.to_csv(f'C:\\users\\ricky\\desktop\\google_sheet_data.csv', index=False)
    # cogs_df.to_csv(f'C:\\users\\ricky\\desktop\\google_sheet_data_1.csv', index=False)
    data = []

    # new_df = new_df.iloc[45:46]
    # print(new_df['LOCATION STATUS'].unique())
    for i in range(new_df.shape[0]):
        po_status = str(new_df['PO TYPE'].iloc[i])
        related_product = str(new_df['RECORD_ID'].iloc[i])
        shipping_with = str(new_df['SHIPPED WITH'].iloc[i])
        invoice_num = str(new_df['INVOICE #'].iloc[i])
        contact_person = str(new_df['CONTACT PERSON'].iloc[i])
        qty = str(new_df['QTY'].iloc[i])

        date_order_placed = new_df['ORDER PLACED'].iloc[i]
        date_order_placed = date_order_placed.strftime('%Y-%m-%d') if str(date_order_placed) != 'NaT' else ""
        unit_price = str(new_df['UNIT PRICE'].iloc[i])
        deposit_sent = str(new_df['DEPOSIT SENT'].iloc[i])
        confirmation_number = str(new_df['CONFIRMATION #'].iloc[i])

        date_last_paid = new_df['DATE PAID'].iloc[i]
        date_last_paid = date_last_paid.strftime('%Y-%m-%d') if str(date_last_paid) != 'NaT' else ""

        # date_last_paid = str(new_df['DATE PAID'].iloc[i])

        container_number = str(new_df['CONTAINER #'].iloc[i])
        production_status = str(new_df['PRODUCTION STATUS'].iloc[i])
        shipping_to = str(new_df['SHIPPING TO'].iloc[i])
        shipping_to_location = str(new_df['SHIPPING TO LOCATION'].iloc[i])
        fba_shipment_id = str(new_df['FBA SHIPMENT ID'].iloc[i])
        deliver_to = str(new_df['DELIVER TO'].iloc[i])
        trucking_by = str(new_df['TRUCKING BY'].iloc[i])
        total_cartons = str(new_df['TOTAL CARTONS'].iloc[i])
        cbm = str(new_df['CBM'].iloc[i])
        etd = new_df['ETD'].iloc[i]
        etd = etd.strftime('%Y-%m-%d') if str(etd) != 'NaT' else ""
        eta = new_df['ETA'].iloc[i]
        eta = eta.strftime('%Y-%m-%d') if str(eta) != 'NaT' else ""
        inventory_location = str(new_df['LOCATION STATUS'].iloc[i])
        status_notes = str(new_df['REMARKS'].iloc[i])
        master_bill_of_lading = str(new_df['MASTER BILL OF LADING #'].iloc[i])

        completion_date = new_df['COMPLETION DATE'].iloc[i]
        completion_date = completion_date.strftime('%Y-%m-%d') if str(completion_date) != 'NaT' else ""

        duties = str(new_df['DUTIES'].iloc[i])
        customs_fees= str(new_df['CUSTOMS_FEES'].iloc[i])
        demmurage= str(new_df['DEMURRAGE'].iloc[i])
        container_cost= str(new_df['CONTAINER_COST'].iloc[i])
        trucking_cost= str(new_df['TRUCKING_COST'].iloc[i])
        confirmation_for_payment = str(new_df['CONFIRMATION # 1'].iloc[i])

        # print(completion_date)

        # completion_date = completion_date.strftime('%Y-%m-%d') if completion_date != "" else completion_date


        sku = str(new_df['SKU'].iloc[i])
        account_name  = str(new_df['ACCOUNT_NAME'].iloc[i])
        asin = str(new_df['ASIN'].iloc[i])


        body = {
            x.upload_data.factory_po_fields.related_product: {"value": related_product},
            x.upload_data.factory_po_fields.shipping_with: {"value": shipping_with},
            x.upload_data.factory_po_fields.invoice_num: {"value": invoice_num},
            x.upload_data.factory_po_fields.contact_person: {"value": contact_person},
            x.upload_data.factory_po_fields.qty: {"value": qty},
            x.upload_data.factory_po_fields.date_order_placed: {"value": date_order_placed},
            x.upload_data.factory_po_fields.unit_price: {"value": unit_price},
            x.upload_data.factory_po_fields.deposit_sent: {"value": deposit_sent},
            x.upload_data.factory_po_fields.confirmation_number: {"value": confirmation_number},
            x.upload_data.factory_po_fields.date_last_paid: {"value": date_last_paid},
            x.upload_data.factory_po_fields.production_status: {"value": production_status},
            x.upload_data.factory_po_fields.shipping_to: {"value": shipping_to},
            x.upload_data.factory_po_fields.shipping_to_location: {"value": shipping_to_location},
            x.upload_data.factory_po_fields.fba_shipment_id: {"value": fba_shipment_id},
            x.upload_data.factory_po_fields.deliver_to: {"value": deliver_to},
            x.upload_data.factory_po_fields.trucking_by: {"value": trucking_by},
            x.upload_data.factory_po_fields.total_cartons: {"value": total_cartons},
            x.upload_data.factory_po_fields.cbm: {"value": cbm},
            x.upload_data.factory_po_fields.etd: {"value": etd},
            x.upload_data.factory_po_fields.eta: {"value": eta},
            x.upload_data.factory_po_fields.inventory_location: {"value": inventory_location},
            x.upload_data.factory_po_fields.status_notes: {"value": status_notes},
            x.upload_data.factory_po_fields.master_bill_of_lading: {"value": master_bill_of_lading},
            x.upload_data.factory_po_fields.completion_date: {"value": completion_date},
            x.upload_data.factory_po_fields.sku: {"value": sku},
            x.upload_data.factory_po_fields.account_name: {"value": account_name},
            x.upload_data.factory_po_fields.asin: {"value": asin},
            x.upload_data.factory_po_fields.po_status: {"value": po_status},
            x.upload_data.factory_po_fields.container_number: {"value": container_number},
            x.upload_data.factory_po_fields.duties: {"value": duties},
            x.upload_data.factory_po_fields.customs_fees: {"value": customs_fees},
            x.upload_data.factory_po_fields.demmurage: {"value": demmurage},
            x.upload_data.factory_po_fields.container_cost: {"value": container_cost},
            x.upload_data.factory_po_fields.trucking_cost: {"value": trucking_cost},
            x.upload_data.factory_po_fields.confirmation_for_payment: {"value": confirmation_for_payment},


        }
        data.append(body)
        # break
        #
    print_color(data, color='b')
    if len(data) > 0:
        each_filter = account_name
        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).purge_table_records(
            table_id=x.factory_pos_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.qb_username, password=x.qb_password,
            filter_val=each_filter,
            reference_column=x.upload_data.factory_po_fields.account_name
        )

        QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(
            table_id=x.factory_pos_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
            username=x.username, password=x.password, filter_val=None, update_type='add_record',
            data=data, reference_column=None)


def import_factory_pos(x, engine):
    quickbase_po_data, po_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                               app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.factory_pos_table_id,
        col_order=x.upload_data.factory_po_fields.col_order,
        filter=x.upload_data.factory_po_fields.filter,
        field_id=x.upload_data.factory_po_fields.field_id,
        filter_type=x.upload_data.factory_po_fields.filter_type,
        filter_operator=x.upload_data.factory_po_fields.filter_operator
    )

    print_color(quickbase_po_data,  color='r')
    table_name = 'ygb_quickbase_po_data'
    if inspect(engine).has_table(table_name):
        engine.connect().execute(f'Drop Table if exists {table_name}')
    sql_types = Get_SQL_Types(quickbase_po_data).data_types
    Change_Sql_Column_Types(engine=engine, Project_name=x.project_name, Table_Name=table_name,
                                             DataTypes=sql_types, DataFrame=quickbase_po_data)
    quickbase_po_data.to_sql(name=table_name, con=engine, if_exists='append', index=False, schema=x.project_name, chunksize=1000,
              dtype=sql_types)

    print_color(f'Quickbase PO Data imported to SQL', color='y')


def factory_order_assignments(x, engine):
    QBapi = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id)
    quickbase_product_data, product_column_dict = QBapi.get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_data.product_fields.col_order,
        filter=x.upload_data.product_fields.filter,
        field_id=x.upload_data.product_fields.field_id,
        filter_type=x.upload_data.product_fields.filter_type,
        filter_operator=x.upload_data.product_fields.filter_operator
    )
    record_id_column = product_column_dict.get(x.upload_data.product_fields.record_id)
    account_name_column = product_column_dict.get(x.upload_data.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_data.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_data.product_fields.asin)

    quickbase_product_data = quickbase_product_data[[record_id_column, account_name_column, sku_column, asin_column]]
    quickbase_product_data.columns = ['RECORD_ID', 'ACCOUNT_NAME', 'SKU', 'ASIN']
    print_color(quickbase_product_data, color='r')
    quickbase_product_data['SKU'] = quickbase_product_data['SKU'].str.upper()


    '''GET PO DATA'''
    # quickbase_po_data, po_column_dict = QBapi.get_qb_table_records(
    #     table_id=x.factory_pos_table_id,
    #     col_order=x.upload_data.factory_po_fields.col_order,
    #     filter=x.upload_data.factory_po_fields.filter,
    #     field_id=x.upload_data.factory_po_fields.field_id,
    #     filter_type=x.upload_data.factory_po_fields.filter_type,
    #     filter_operator=x.upload_data.factory_po_fields.filter_operator
    # )
    #
    #
    # quickbase_po_data.columns = [x.upper() for x in quickbase_po_data.columns]
    # quickbase_po_data['ACCOUNT_NAME'] = quickbase_po_data['ACCOUNT_NAME'].str.upper()
    # quickbase_po_data['ASIN'] = quickbase_po_data['ASIN'].str.upper()
    # quickbase_po_data['SKU'] = quickbase_po_data['SKU'].str.upper()
    # quickbase_po_data['FBA_SHIPMENT_ID'] = quickbase_po_data['FBA_SHIPMENT_ID'].str.upper()
    # quickbase_po_data = quickbase_po_data[['RECORD_ID_NUM', 'ACCOUNT_NAME', 'ASIN', 'SKU', 'FBA_SHIPMENT_ID', 'PO_STATUS']]
    #
    # print_color(quickbase_po_data, color='y')
    # quickbase_po_data = quickbase_po_data[quickbase_po_data['PO_STATUS']=='Completed']

    ''' GET SQL DATA '''
    df = pd.read_sql(f'''select * from ygb_inventory_ledger_assignment''', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df = df.replace(np.nan, "")
    # new_df = pd.concat([reference_df, df], sort=False).drop_duplicates(subset=['ACCOUNT_NAME', 'SHIPMENTID'],
    #                                                                    keep=False)

    df.columns = [x.upper() for x in df.columns]
    df['ACCOUNT_NAME'] = df['ACCOUNT_NAME'].str.upper()
    df['ASIN'] = df['ASIN'].str.upper()
    df['SKU'] = df['SKU'].str.upper()
    df['FBA_SHIPMENT_ID'] = df['FBA_SHIPMENT_ID'].str.upper()

    print_color(df, color='y')
    print_color(df.shape[0], color='y')
    df = df.merge(quickbase_product_data, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'ASIN'], right_on=['ACCOUNT_NAME', 'SKU', 'ASIN'])
    # df = df.merge(quickbase_po_data, how='left', left_on=['ACCOUNT_NAME', 'SKU', 'FBA_SHIPMENT_ID'], right_on=['ACCOUNT_NAME', 'SKU', 'FBA_SHIPMENT_ID'])
    # quickbase_po_data.to_csv(f'C:\\users\\{getpass.getuser()}\\desktop\\assignment_data.csv', index=False)
    df.rename(columns={'RECORD_ID': 'PRODUCT_RECORD_ID'}, inplace=True)
    # df.rename(columns={'RECORD_ID_NUM': 'FBA_SHIPMENT_RECORD_ID'}, inplace=True)
    # print_color(df.columns, color='r')


    QBapi.purge_table_records(table_id=x.unit_ledger_table_id, user_token=x.qb_user_token, apptoken=x.qb_app_token,
                              username=x.qb_username, password=x.qb_password,
                              filter_val="YGB Group",
                              reference_column=x.upload_data.unit_ledger_fields.account_name,
                              filter_type="EX")
    print_color(df.shape[0], color='y')
    print_color(df.columns, color='y')

    counter = 0

    for i in range(0, df.shape[0], 1000):
        new_df = df.loc[i:i + 999]
        qb_data = []
        for j in range(new_df.shape[0]):
            account_name = new_df['ACCOUNT_NAME'].iloc[j]
            date = new_df['DATE'].iloc[j].strftime('%Y-%m-%d')
            amazon_order_id = new_df['AMAZON-ORDER-ID'].iloc[j]
            sku = new_df['SKU'].iloc[j]
            asin = new_df['ASIN'].iloc[j]
            order_status = new_df['ORDER-STATUS'].iloc[j]
            item_status = new_df['ITEM-STATUS'].iloc[j]
            fba_shipment_id = new_df['FBA_SHIPMENT_ID'].iloc[j]
            quantity = str(new_df['QUANTITY'].iloc[j])
            related_product = str(new_df['PRODUCT_RECORD_ID'].iloc[j])
            related_factory_order = str(new_df['RECORD_ID_NUM'].iloc[j])
            unit_price = str(new_df['UNIT_PRICE'].iloc[j])
            duties = str(new_df['DUTIES'].iloc[j])
            customs_fees = str(new_df['CUSTOMS_FEES'].iloc[j])
            demmurage = str(new_df['DEMMURAGE'].iloc[j])
            container_cost = str(new_df['CONTAINER_COST'].iloc[j])
            trucking_cost = str(new_df['TRUCKING_COST'].iloc[j])

            body = {

                x.upload_data.unit_ledger_fields.account_name: {"value": account_name},
                x.upload_data.unit_ledger_fields.date: {"value": date},
                x.upload_data.unit_ledger_fields.amazon_order_id: {"value": amazon_order_id},
                x.upload_data.unit_ledger_fields.sku: {"value": sku},
                x.upload_data.unit_ledger_fields.asin: {"value": asin},
                x.upload_data.unit_ledger_fields.order_status: {"value": order_status},
                x.upload_data.unit_ledger_fields.item_status: {"value": item_status},
                x.upload_data.unit_ledger_fields.fba_shipment_id: {"value": fba_shipment_id},
                x.upload_data.unit_ledger_fields.quantity: {"value": quantity},
                x.upload_data.unit_ledger_fields.related_product: {"value": related_product},
                x.upload_data.unit_ledger_fields.related_factory_order: {"value": related_factory_order},
                x.upload_data.unit_ledger_fields.unit_price: {"value": unit_price},
                x.upload_data.unit_ledger_fields.duties: {"value": duties},
                x.upload_data.unit_ledger_fields.customs_fees: {"value": customs_fees},
                x.upload_data.unit_ledger_fields.demmurage: {"value": demmurage},
                x.upload_data.unit_ledger_fields.container_cost: {"value": container_cost},
                x.upload_data.unit_ledger_fields.trucking_cost: {"value": trucking_cost}
            }
            qb_data.append(body)
            # break
        print_color(qb_data, color='g')
        print_color(f'Count of qb_data: {len(qb_data)}', color='g')
        if len(qb_data) > 0:
            QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.unit_ledger_table_id,
                                                                                        user_token=x.qb_user_token,
                                                                                        apptoken=x.qb_app_token,
                                                                                        username=x.username,
                                                                                        password=x.password,
                                                                                        filter_val=None,
                                                                                        update_type='add_record',
                                                                                        data=qb_data,
                                                                                        reference_column=None)

            print_color(f'Batch {counter} Uploaded', color='G')
        counter += 1

        # break

