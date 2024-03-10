drop temporary table if exists current_all_listings;
create temporary table if not exists current_all_listings(primary key(ACCOUNT_NAME, asin1,`SELLER-SKU` ))
select * from
(select *, row_number() over (partition by ASIN1, `SELLER-SKU`  order by `OPEN-DATE` desc) as ranking from all_listings) A
where ranking =1;


alter table current_all_listings rename column asin1 to asin;
alter table current_all_listings rename column `SELLER-SKU` to sku;
alter table current_all_listings modify column `ITEM-NAME` text;

drop temporary table if exists missing_products;
create temporary table if not exists  missing_products
select ACCOUNT_NAME, `ITEMNAME`, SKU, LIST_PRICE, asin from
product_data A left join (select * from current_all_listings) B using(ACCOUNT_NAME, asin, SKU)
where b.asin is null;


insert into current_all_listings(`ACCOUNT_NAME`, `ITEM-NAME`, sku,  PRICE, asin)
select * from missing_products;


drop table if exists quickbase_product_data;
create table if not exists quickbase_product_data
select a.account_name, a.asin,b.asins as parent_asin,  a.SKU,a.`LISTING-ID`,a.`ITEM-NAME`, a.`ITEM-DESCRIPTION`, a.PRICE, a.`OPEN-DATE`, a.`PRODUCT-ID`, a.`FULFILLMENT-CHANNEL`, a.STATUS ,
B.style, b.color, b.size, C.FNSKU
from current_all_listings A
left join (select * from product_data where asin != "" and asin != "0" group by account_name, asin, sku) B on  A.account_name = b.account_name and a.asin = b.asin and  a.sku = b.sku
left join (select account_name, sku, fnsku from all_inventory group by  account_name, sku) C on  A.account_name = C.account_name and a.sku = c.sku ;
-- where  a.STATUS != "Incomplete";

set @max_date = (select max(date) from ledger_summary_view);
drop table if exists quickbase_unfulfilable_data;
create table if not exists quickbase_unfulfilable_data(primary key( ACCOUNT_NAME, ASIN, SKU))
select 
ACCOUNT_NAME, DATE, FNSKU, ASIN, MSKU as SKU, TITLE,
sum(case when DISPOSITION = "DEFECTIVE" then `ENDING WAREHOUSE BALANCE` else null end) as DEFECTIVE,
sum(case when DISPOSITION = "WAREHOUSE_DAMAGED" then `ENDING WAREHOUSE BALANCE` else null end) as WAREHOUSE_DAMAGED,
sum(case when DISPOSITION = "CUSTOMER_DAMAGED" then `ENDING WAREHOUSE BALANCE` else null end) as CUSTOMER_DAMAGED
from ledger_summary_view where date = @max_date
group by ACCOUNT_NAME, ASIN, MSKU;

drop table if exists quickbase_inventory;
create table if not exists quickbase_inventory
select 
ACCOUNT_NAME, SKU, ASIN, 
`CONDITION`, 
`YOUR-PRICE` as Price,
`MFN-LISTING-EXISTS` AS mfn_listing_exists,
`AFN-LISTING-EXISTS` AS afn_listing_exists,
`MFN-FULFILLABLE-QUANTITY` AS mfn_fulfillbale_qty,
`AFN-WAREHOUSE-QUANTITY` as afn_warehouse_qty, 
`AFN-FULFILLABLE-QUANTITY` as afn_fulfillable_qty,
`AFN-UNSELLABLE-QUANTITY` as afb_unsellable_qty,
DEFECTIVE, 
WAREHOUSE_DAMAGED, 
CUSTOMER_DAMAGED,
ifnull(`AFN-INBOUND-WORKING-QUANTITY`,0) + ifnull(`AFN-INBOUND-SHIPPED-QUANTITY`,0) + ifnull(`AFN-INBOUND-RECEIVING-QUANTITY`,0) as AFN_Inbound_qty,
`AFN-INBOUND-WORKING-QUANTITY` as afn_inbound_working,
`AFN-INBOUND-SHIPPED-QUANTITY`  as afn_inbound_shipped,
`AFN-INBOUND-RECEIVING-QUANTITY`  as afn_inbound_receiving,
`AFN-RESEARCHING-QUANTITY` as afn_researching_qty,
RESERVED_QTY as afn_reserved_qty,
RESERVED_CUSTOMERORDERS as afn_reserved_customer_orders, 
`RESERVED_FC-TRANSFERS` as afn_reserved_fc_transfers, 
`RESERVED_FC-PROCESSING`as afn_reserved_fc_processing, 
`AFN-TOTAL-QUANTITY` as  afn_total_qty
from all_inventory a 
left join reserved_inventory B using(account_name, sku, asin)
left join quickbase_unfulfilable_data c using(account_name, sku, asin);