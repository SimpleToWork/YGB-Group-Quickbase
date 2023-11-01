use ybg_group_amazon_seller_central;


drop table if exists ygb_quickbase_fulfilled_order_data;
create table if not exists ygb_quickbase_fulfilled_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`, `SHIPMENT-ID`))
select account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`, "shipped" as `ITEM-STATUS`, round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) as `ITEM-PRICE`, sum(`QUANTITY-SHIPPED`) as Quantity  
from amazon_fulfilled_shipments 
-- where `AMAZON-ORDER-ID` =  "111-1457537-2870604"
group by  account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`,round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) ;  


drop table if exists  ygb_quickbase_assigned_order_data;
create table if not exists ygb_quickbase_assigned_order_data(primary key (id))
select 
row_number() over () as ID, 
row_number() over (partition by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`, `SHIPMENT-ID` order by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`) as New_Ranking,
a.* , b.`SHIPMENT-ID`, b.Quantity AS fulfilled_Quantity
from ygb_quickbase_order_data A 
left join ygb_quickbase_fulfilled_order_data B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and A.`AMAZON-ORDER-ID` = B.`AMAZON-ORDER-ID` and A.SKU = B.SKU and A. `ITEM-STATUS` = B. `ITEM-STATUS` and A.`UNIT-PRICE` = B.`ITEM-PRICE`
where date(A.`PURCHASE-DATE`) >= "2023-01-01"
-- where A.`AMAZON-ORDER-ID` = "111-2301076-9064201"
;
-- where A.`ITEM-STATUS` = "shipped";

create index unique_lookup on ygb_quickbase_assigned_order_data(ACCOUNT_NAME, `AMAZON-ORDER-ID`, SKU);


-- select * from ygb_quickbase_assigned_order_data  limit 100;
-- Alter table combined_quickbase_settlement_order_data modify Quantity int;
-- Alter table ygb_quickbase_assigned_order_data modify Quantity int;

drop table if exists ygb_quickbase_assigned_order_data_missing_shipments;
create table if not exists ygb_quickbase_assigned_order_data_missing_shipments(primary key(id, ranking))
select
b.id, 
row_number() over (partition by  a.account_name, `ORDER-ID`,a.SKU order by  a.account_name, `ORDER-ID`,a.SKU) as ranking,
 a.account_name, `ORDER-ID`,a.SKU, Group_ID as `SHIPMENT-ID`, "Shipped" as `ITEM-STATUS`, Principal as `ITEM-PRICE`, A.Quantity
from combined_quickbase_settlement_order_data A inner join 
(select * from ygb_quickbase_assigned_order_data A where  A.`ITEM-STATUS` = "shipped"
and a.`SHIPMENT-ID` is null and date(`PURCHASE-DATE`) >= "2023-01-01") B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and A. `ORDER-ID` = B.`AMAZON-ORDER-ID` and A.SKU = B.SKU
and `TRANSACTION-TYPE` ="Shipped";

create index id on ygb_quickbase_assigned_order_data_missing_shipments(id);

set @max_id = (select max(id) from ygb_quickbase_assigned_order_data);

drop table if exists ygb_quickbase_assigned_orders_to_replace;
create table if not exists ygb_quickbase_assigned_orders_to_replace
select 
@max_id + row_number() over (partition by "") as ID, 
row_number() over (partition by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`, `SHIPMENT-ID` order by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`) as New_Ranking,
 A.Ranking, A.ACCOUNT_NAME, A.QUANTITY, `PURCHASE-DATE`, A.`ITEM-PRICE`, A.`UNIT-PRICE`, ASIN, 
`AMAZON-ORDER-ID`, `MERCHANT-ORDER-ID`, `ORDER-STATUS`, `FULFILLMENT-CHANNEL`, `SALES-CHANNEL`, `ORDER-CHANNEL`, `SHIP-SERVICE-LEVEL`, 
`PRODUCT-NAME`, A.SKU, A.`ITEM-STATUS`, CURRENCY, `ITEM-TAX`, `SHIPPING-PRICE`, `SHIPPING-TAX`, `GIFT-WRAP-PRICE`, `GIFT-WRAP-TAX`, 
`ITEM-PROMOTION-DISCOUNT`, `SHIP-PROMOTION-DISCOUNT`, `SHIP-CITY`, `SHIP-STATE`, `SHIP-POSTAL-CODE`, `SHIP-COUNTRY`, `PROMOTION-IDS`, 
`IS-BUSINESS-ORDER`, `PURCHASE-ORDER-NUMBER`, `PRICE-DESIGNATION`, `IS-TRANSPARENCY`, `SIGNATURE-CONFIRMATION-RECOMMENDED`, 
B.`SHIPMENT-ID`, B.Quantity as fulfilled_Quantity
from ygb_quickbase_assigned_order_data A
inner join ygb_quickbase_assigned_order_data_missing_shipments B using(id)
where A.`ITEM-STATUS` = "shipped"
and a.`SHIPMENT-ID` is null and date(`PURCHASE-DATE`) >= "2023-01-01";

delete A.* from ygb_quickbase_assigned_order_data A inner join  ygb_quickbase_assigned_order_data_missing_shipments using(id) ;

insert into ygb_quickbase_assigned_order_data
select * from ygb_quickbase_assigned_orders_to_replace;

drop table if exists ygb_quickbase_final_assigned_orders;
create table if not exists ygb_quickbase_final_assigned_orders
select A.*, B.FBA_Fee, B.Commission, B.Principal from ygb_quickbase_assigned_order_data A 
left join combined_quickbase_settlement_order_data B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and  A.`AMAZON-ORDER-ID` = B.`ORDER-ID` 
and A.sku = B.sku and A.`ORDER-STATUS` = B.`TRANSACTION-TYPE` and  A.`SHIPMENT-ID` = B.Group_ID and A.new_ranking = B.ranking;
-- where date(`PURCHASE-DATE`) > "2023-01-01";
-- and `ORDER-STATUS` = "Shipped"
-- and `ITEM-STATUS` = "Shipped" 
-- and `SHIPMENT-ID` is not null
-- and FBA_Fee is null;

update ygb_quickbase_final_assigned_orders set FBA_Fee = 0.00 where FBA_Fee is null;
update ygb_quickbase_final_assigned_orders set Commission = 0.00 where Commission is null;
update ygb_quickbase_final_assigned_orders set Principal = 0.00 where Principal is null;



-- ----------------------------------------------------------------------------------------
-- Returns ---------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------

drop table if exists ygb_quickbase_return_data;
create table if not exists ygb_quickbase_return_data (primary key( ACCOUNT_NAME, `ORDER-ID`, SKU, `ORDER-STATUS`, Ranking))
select
row_number() over (partition by A.ACCOUNT_NAME, A.`ORDER-ID`, A.SKU order by A.ACCOUNT_NAME, A.`ORDER-ID`, A.SKU) as Ranking,
A.ACCOUNT_NAME,  QUANTITY, `RETURN-DATE` as `PURCHASE-DATE`,null as `ITEM-PRICE`, ASIN, A.`ORDER-ID` ,A.`ORDER-ID` as `MERCHANT-ORDER-ID`,
"Return" as `ORDER-STATUS`, `PRODUCT-NAME`, A.SKU, A.STATUS as `ITEM-STATUS`
from fba_returns A
where `RETURN-DATE` >= "2023-01-01" ;

drop table if exists ygb_quickbase_assigned_return_data;
create table if not exists ygb_quickbase_assigned_return_data
select A.*, B.Group_ID,  B.Order_Line_Units from ygb_quickbase_return_data A left join ygb_quickbase_settlement_returns B
on A.ACCOUNT_NAME = B.ACCOUNT_NAME and A.`Order-ID` = B.`Order-ID` and A.sku = B.sku and A.Ranking = B.new_ranking
where B.Group_ID is not null;




 


-- ----------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------


-- select * from ygb_quickbase_assigned_order_data where date(`PURCHASE-DATE`) > "2023-01-01" and `ORDER-STATUS` = "Shipped" and `ITEM-STATUS` = "Shipped" and `SHIPMENT-ID` is  null;
-- select * from quickbase_settlement_order_data where `ORDER-ID`= "111-0096667-7254667" and "X73686JgL";


-- set @order_id:="113-7066225-5940223";
-- select * from all_orders where `Amazon-ORDER-ID` =  @order_id;
-- select * from ygb_quickbase_order_data where `Amazon-ORDER-ID` =  @order_id;
-- select * from ygb_quickbase_fulfilled_order_data  where  `amazon-ORDER-ID` =@order_id;
-- select * from ygb_quickbase_assigned_order_data  where `amazon-ORDER-ID` =@order_id;
-- select * from combined_quickbase_settlement_order_data  where `order-id` = @order_id;	
-- select * from ygb_quickbase_assigned_orders_to_replace  where `order-id` = @order_id;	
-- select * from fba_returns where `ORDER-ID` =@order_id;
-- select * from amazon_fulfilled_shipments where  `amazon-ORDER-ID` =@order_id;
-- select * from settlements where `ORDER-ID` =@order_id ;

-- ygb_quickbase_assigned_order_data
-- -- select * from combined_quickbase_settlement_order_data where `TRANSACTION-TYPE` in ("Refund","Chargeback Refund") and status = "posted"  and `order-id` = @order_id;			

-- select * from ygb_quickbase_assigned_order_data;

-- select A.*, B.FBA_Fee, B.Commission, B.Principal from ygb_quickbase_assigned_order_data A 
-- left join combined_quickbase_settlement_order_data B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and  A.`AMAZON-ORDER-ID` = B.`ORDER-ID` 
-- and A.sku = B.sku and A.`ORDER-STATUS` = B.`TRANSACTION-TYPE` and  A.`SHIPMENT-ID` = B.Group_ID and A.new_ranking = B.ranking
-- where date(`PURCHASE-DATE`) > "2023-01-01"
-- and `ORDER-STATUS` = "Shipped"
-- and `ITEM-STATUS` = "Shipped" 
--  and `SHIPMENT-ID` is not null
--  and FBA_Fee is null;
--  
--  
-- and `AMAZON-ORDER-ID`  = @order_id;


-- select * from ygb_quickbase_assigned_return_data  where date(`PURCHASE-DATE`) > "2023-01-01";

-- select * from ygb_quickbase_assigned_order_data where date(`PURCHASE-DATE`) > "2023-01-01";
-- select * from combined_quickbase_settlement_order_data limit 1000;
-- select * from ygb_quickbase_return_data where `ORDER-ID` = "114-7480084-4368216";
-- select * from ygb_quickbase_settlement_returns  where `ORDER-ID` = "114-7480084-4368216";
-- select * from ygb_quickbase_settlement_returns;


-- select ACCOUNT_NAME, `AMAZON-ORDER-ID`, SKU, count(*) from ygb_quickbase_return_data group by ACCOUNT_NAME, `AMAZON-ORDER-ID`, SKU;


-- SELECT * FROM  fba_returns A
-- where `RETURN-DATE` >= "2023-01-01" ;
-- create index unique_level on ygb_quickbase_return_data( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`);

-- drop table if exists ygb_quickbase_fulfilled_order_data;
-- create table if not exists ygb_quickbase_fulfilled_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`, `SHIPMENT-ID`))
-- select account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`, "shipped" as `ITEM-STATUS`, round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) as `ITEM-PRICE`, sum(`QUANTITY-SHIPPED`) as Quantity  
-- from amazon_fulfilled_shipments 
-- -- where `AMAZON-ORDER-ID` =  "111-1457537-2870604"
-- group by  account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`,round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) ;  

-- select `ORDER-ID`, SKU, Group_ID, count(*) from combined_quickbase_settlement_order_data where `TRANSACTION-TYPE` in ("Refund","Chargeback Refund") and status = "posted"
-- group  by `ORDER-ID`, SKU, Group_ID;


