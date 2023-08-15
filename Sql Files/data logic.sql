drop temporary table if exists current_all_listings;
create temporary table if not exists current_all_listings(primary key(ACCOUNT_NAME, asin1,`SELLER-SKU` ))
select * from
(select *, row_number() over (partition by ASIN1, `SELLER-SKU`  order by `OPEN-DATE` desc) as ranking from all_listings) A
where ranking =1;

alter table current_all_listings rename column asin1 to asin;

drop table if exists quickbase_product_data;
create table if not exists quickbase_product_data
select a.account_name, a.asin,b.asins as parent_asin,  a.`SELLER-SKU`,a.`LISTING-ID`,a.`ITEM-NAME`, a.`ITEM-DESCRIPTION`, a.PRICE, a.`OPEN-DATE`, a.`PRODUCT-ID`, a.`FULFILLMENT-CHANNEL`, a.STATUS ,
B.style, b.color, b.size, C.FNSKU
from current_all_listings A
left join product_data B on  A.account_name = b.account_name and a.`SELLER-SKU` = b.sku
left join (select account_name, sku, fnsku from all_inventory group by  account_name, sku) C on  A.account_name = C.account_name and a.`SELLER-SKU` = c.sku ;

select * from quickbase_product_data;
select * from current_all_listings
-- create index account_sku on product_data(account_name, sku);
-- create index account_sku on all_listings(account_name, `SELLER-SKU`);
-- create index account_sku on all_inventory(account_name, SKU);


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