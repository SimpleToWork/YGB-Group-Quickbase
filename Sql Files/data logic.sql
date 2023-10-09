drop temporary table if exists current_all_listings;
create temporary table if not exists current_all_listings(primary key(ACCOUNT_NAME, asin1,`SELLER-SKU` ))
select * from
(select *, row_number() over (partition by ASIN1, `SELLER-SKU`  order by `OPEN-DATE` desc) as ranking from all_listings) A
where ranking =1;


alter table current_all_listings rename column asin1 to asin;
alter table current_all_listings rename column `SELLER-SKU` to sku;

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


-- select * from quickbase_inventory;

-- select * from all_inventory;

-- select distinct account_name, sku, ASIN from reserved_inventory;
-- select * from afn_inventory_data;
-- select * from all_inventory;
-- select * from bulk_fix_stranded_inventory;
-- select * from fba_inventory_planning;
-- select * from fba_small_light_inventory;
-- select * from reserved_inventory;
-- select * from restock_inventory_reccomendations;
-- select * from restock_inventory_recommendations;
-- select * from stranded_inventory;
-- select * from unsupressed_inventory;

-- show tables;

-- select * from ledger_detail_view  where asin = "B0828N4WTX";
-- select * from ledger_summary_view where MSKU = "3W-9BHO-WF4J";

-- 




-- SELECT * FROM 

-- explain
-- select * from all_inventory a left join reserved_inventory B using(account_name, sku, asin)
-- select asin, sku, count(*) from quickbase_product_data group by asin, sku;
-- select * from current_all_listings;

-- select * from all_orders limit 1000;
-- select  
-- ACCOUNT_NAME,
-- QUANTITY,
-- `PURCHASE-DATE
-- `ITEM-PRICE
-- ASIN,
-- `AMAZON-ORDER-ID
-- `MERCHANT-ORDER-ID`,
-- `ORDER-STATUS`,
-- `FULFILLMENT-CHANNEL`,
-- `SALES-CHANNEL`,
-- `ORDER-CHANNEL`,
-- `SHIP-SERVICE-LEVEL`,
-- `PRODUCT-NAME`,
-- SKU,
-- `ITEM-STATUS`,
-- CURRENCY,
-- `ITEM-TAX`,
-- `SHIPPING-PRICE`,
-- `SHIPPING-TAX`,
-- `GIFT-WRAP-PRICE`,
-- `GIFT-WRAP-TAX`,
-- `ITEM-PROMOTION-DISCOUNT`,
-- `SHIP-PROMOTION-DISCOUNT`,
-- `SHIP-CITY`,
-- `SHIP-STATE`,
-- `SHIP-POSTAL-CODE`,
-- `SHIP-COUNTRY`,
-- `PROMOTION-IDS`,
-- `IS-BUSINESS-ORDER`,
-- `PURCHASE-ORDER-NUMBER`,
-- `PRICE-DESIGNATION`,
-- `IS-TRANSPARENCY`,
-- `SIGNATURE-CONFIRMATION-RECOMMENDED`
-- from all_orders where  `PURCHASE-DATE` >= "2023-01-01" and `PURCHASE-DATE` < "2023-01-02";
 
--  select * from all_orders limit 100;

-- select date, lag(date,1) over (partition by "" order by date) as last_date, datediff(date,  lag(date,1) over (partition by "" order by date)) as date_difference, count from
-- (select date(`PURCHASE-DATE`) as date, count(*) as count from all_orders group by date(`PURCHASE-DATE`)) A;


-- select * from all_orders where `ORDER-STATUS` in ("pending", "shipping");
-- select * from report_que where table_name = "all_orders_by_last_update";
-- create index account_sku on product_data(account_name, sku);
-- create index account_sku on all_listings(account_name, `SELLER-SKU`);
-- create index account_sku on all_inventory(account_name, SKU);


-- select * from report_que where report_requested = 0;

-- select * from all_listings where asin1 = "B09QG4FH6K" group by `LISTING-ID`;
-- select * from active_listings where asin1 = "B09QG4FH6K" group by `LISTING-ID`;
-- select * from inactive_listings where asin1 = "B09QG4FH6K" group by `LISTING-ID`;




-- select * from
-- (select *, 
-- row_number() over (partition by asin, `LISTING-ID`  order by case when checkpoint != 0 then 1 else 2 end, `OPEN-DATE` desc) as ranking
--  from
-- (select account_name, asin,`LISTING-ID` ,
-- case when  sum(checkpoint) <= 0 then "Inactive" else "Active" end as status, sum(checkpoint) as checkpoint, `OPEN-DATE`, `PRICE`, `SELLER-SKU`, `ITEM-NAME`, `PRODUCT-ID`, `FULFILLMENT-CHANNEL`
-- from
-- (select account_name, asin1 as asin,`LISTING-ID` ,  "Inactive" as status, -1 as checkpoint, `OPEN-DATE`, `PRICE`, `SELLER-SKU`, `ITEM-NAME`, `PRODUCT-ID`, `FULFILLMENT-CHANNEL` from inactive_listings
-- union
-- select account_name, asin1 as asin, `LISTING-ID` , "Active" as status, 1 as checkpoint, `OPEN-DATE`, `PRICE`, `SELLER-SKU`, `ITEM-NAME`, `PRODUCT-ID`, `FULFILLMENT-CHANNEL` from active_listings) A
-- group by account_name, asin, `LISTING-ID` ) A) B
-- where ranking = 1;



-- drop temporary table if exists current_active_listings;
-- create temporary table if not exists current_active_listings(primary key(ACCOUNT_NAME, asin1, `LISTING-ID` ))
-- select * from
-- (select *, row_number() over (partition by asin1, `LISTING-ID` order by `OPEN-DATE` desc) as ranking from active_listings) A
-- where ranking = 1;

-- alter table current_active_listings rename column asin1 to asin;

-- drop temporary table if exists current_inactive_listings;
-- create temporary table if not exists current_inactive_listings(primary key(ACCOUNT_NAME, asin1, `LISTING-ID` ))
-- select * from
-- (select *, row_number() over (partition by asin1, `LISTING-ID` order by `OPEN-DATE` desc) as ranking from inactive_listings) A
-- where ranking = 1;

-- alter table current_inactive_listings rename column asin1 to asin;



-- drop temporary table if exists current_listings;
-- create temporary table if not exists current_listings(primary key(ACCOUNT_NAME, asin))
-- select distinct account_name, asin from product_data where asin not in ("0", "Unknow", "Unknown");


-- select * from product_data;



-- select * from quickbase_product_data where parent_asin like '%,%'

-- select * from all_inventory

-- SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME IN('fnsku') AND TABLE_SCHEMA = 'ybg_group_amazon_seller_central';

-- select * from product_data_detail;

-- select * from all_listings;

-- (select *, row_number() over (partition by A.ACCOUNT_NAME,  A.ASIN order by priority) as ranking from
-- (select A.ACCOUNT_NAME, A.ASIN, A.SKU,A.SIZE, B.`PRODUCT-ID`, A.COLOR, A.ITEMNAME as description, 
-- A.brand, A.LIST_PRICE, B.`FULFILLMENT-CHANNEL`, `LISTING-ID`   , 
--  case when B.ASIN1 is not null then 1 else 2 end as priority
--  
--  from product_data A 
--  left join all_listings b on A.ACCOUNT_NAME = B.ACCOUNT_NAME and a.SKU = B.`SELLER-SKU`
--  left join all_inventory c on A.ACCOUNT_NAME = c.ACCOUNT_NAME and a.SKU = c.SKU) A) B
--  where ranking = 1;
--  
--  
--  select * from product_data where asin not in ("0", "Unknow", "Unknown") and asin is not null group by ASIN;
--  

-- select * from all_orders;
-- show columns from all_orders;

-- select *, lag(date_time, 1) over (partition by "" order by date_time) as last_time, 
-- timediff(lag(date_time, 1) over (partition by "" order by date_time), date_time) as Time_dfference
--  from  
-- (select date_time, count(*) from
-- (select `PURCHASE-DATE`, 
-- STR_TO_DATE(concat(year(`PURCHASE-DATE`) ,"-", month(`PURCHASE-DATE`),"-", day(`PURCHASE-DATE`)," ", hour(`PURCHASE-DATE`)), "%Y-%m-%d %H") as date_time from all_orders) A
-- group by date_time) A;


--  

-- select A.*, B.* from
-- ( select distinct account_name,  ASIN
-- from all_orders ) A
-- left join 
-- ( select distinct account_name, asin from product_data) B using(account_name, asin);

--  
--  select 
--  
--  
--  
--  
--  