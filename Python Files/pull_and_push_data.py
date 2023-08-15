
import pandas as pd
import numpy as np
from global_modules import print_color
from quickbase_class import QuickbaseAPI

def upload_product_data(x, engine):
    df = pd.read_sql(f'Select * from quickbase_product_data', con=engine)
    df.columns = [x.upper() for x in df.columns]
    df = df.replace(np.nan, "")
    print_color(df, color='y')

    quickbase_data, product_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_products.product_fields.col_order,
        filter=x.upload_products.product_fields.filter,
        field_id=x.upload_products.product_fields.field_id,
        filter_type=x.upload_products.product_fields.filter_type,
        filter_operator=x.upload_products.product_fields.filter_operator
    )

    print_color(quickbase_data, color='r')

    account_name_column = product_column_dict.get(x.upload_products.product_fields.account_name)
    sku_column = product_column_dict.get(x.upload_products.product_fields.sku)
    asin_column = product_column_dict.get(x.upload_products.product_fields.asin)

    if quickbase_data.shape[0] >0:
        reference_df = quickbase_data[[account_name_column, sku_column, asin_column]]
        reference_df.columns = ['ACCOUNT_NAME', 'SKU', 'ASIN']

        new_df = pd.concat(df, reference_df)






    # data = []
    # print_color(df.columns, color='y')
    # for i in range(df.shape[0]):
    #     account_name = df['ACCOUNT_NAME'].iloc[i]
    #     asin = df['ASIN'].iloc[i]
    #     parent_asin = df['PARENT_ASIN'].iloc[i]
    #     sku = df['SELLER-SKU'].iloc[i]
    #     listing_id = df['LISTING-ID'].iloc[i]
    #     item_name = df['ITEM-NAME'].iloc[i]
    #     description = df['ITEM-DESCRIPTION'].iloc[i]
    #     price = df['PRICE'].iloc[i]
    #     open_date = df['OPEN-DATE'].iloc[i].strftime('%Y-%m-%dT%H:%M:%S')
    #     product_id = df['PRODUCT-ID'].iloc[i]
    #     fulfillment_channel = df['FULFILLMENT-CHANNEL'].iloc[i]
    #     status = df['STATUS'].iloc[i]
    #     style = df['STYLE'].iloc[i]
    #     color = df['COLOR'].iloc[i]
    #     size = df['SIZE'].iloc[i]
    #     fnsku = df['FNSKU'].iloc[i]
    #
    #     body = {
    #         x.upload_products.product_fields.product_name: {"value": item_name},
    #         x.upload_products.product_fields.sku: {"value": sku},
    #         x.upload_products.product_fields.parent_asin: {"value": parent_asin},
    #         x.upload_products.product_fields.asin: {"value": asin},
    #         x.upload_products.product_fields.size: {"value": size},
    #         x.upload_products.product_fields.product_id: {"value": product_id},
    #         x.upload_products.product_fields.color: {"value": color},
    #         x.upload_products.product_fields.description: {"value": description},
    #         x.upload_products.product_fields.price: {"value": price},
    #         x.upload_products.product_fields.listing_id: {"value": listing_id},
    #         x.upload_products.product_fields.fnsku: {"value": fnsku},
    #         x.upload_products.product_fields.fulfillment_channel: {"value": fulfillment_channel},
    #         x.upload_products.product_fields.status : {"value": status},
    #         x.upload_products.product_fields.account_name: {"value": account_name},
    #         x.upload_products.product_fields.open_date: {"value": open_date},
    #         x.upload_products.product_fields.style: {"value": style},
    #     }
    #     data.append(body)
    #     # break
    #
    # print_color(data, color='b')
    #
    # QuickbaseAPI(x.qb_hostname, x.qb_auth, x.qb_app_id).create_qb_table_records(table_id=x.product_table_id,
    #     user_token=x.qb_user_token, apptoken=x.qb_app_token, username=x.username, password=x.password,
    #     filter_val=None, update_type='add_record', data=data,
    #     reference_column=None)


def upload_sales_data(x, engine):
    quickbase_data = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.product_table_id,
        col_order=x.upload_products.product_fields.col_order,
        filter=x.upload_products.product_fields.filter,
        field_id=x.upload_products.product_fields.field_id,
        filter_type=x.upload_products.product_fields.filter_type,
        filter_operator=x.upload_products.product_fields.filter_operator
    )

    print_color(quickbase_data, color='r')





