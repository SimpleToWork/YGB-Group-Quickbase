use ybg_group_amazon_seller_central;



create index unique_level on ygb_quickbase_order_data( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`);


drop table if exists ygb_quickbase_fulfilled_order_data;
create table if not exists ygb_quickbase_fulfilled_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`, `SHIPMENT-ID`))
select account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`, "shipped" as `ITEM-STATUS`, round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) as `ITEM-PRICE`, sum(`QUANTITY-SHIPPED`) as Quantity  
from amazon_fulfilled_shipments 
-- where `AMAZON-ORDER-ID` =  "111-1457537-2870604"
group by  account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`,round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) ;  



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
a.* , b.`SHIPMENT-ID`, b.Quantity AS fulfilled_Quantity
from ygb_quickbase_order_data A 
left join ygb_quickbase_fulfilled_order_data B USING( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`);
-- where A.`ITEM-STATUS` = "shipped";

create index unique_lookup on ygb_quickbase_assigned_order_data(ACCOUNT_NAME, `AMAZON-ORDER-ID`, SKU);

-- Alter table combined_quickbase_settlement_order_data modify Quantity int;
-- Alter table ygb_quickbase_assigned_order_data modify Quantity int;


update ygb_quickbase_assigned_order_data A inner join
(select b.id,  a.account_name, `ORDER-ID`,a.SKU, Group_ID as `SHIPMENT-ID`, "Shipped" as `ITEM-STATUS`, Principal as `ITEM-PRICE`, A.Quantity
from combined_quickbase_settlement_order_data A inner join 
(select * from ygb_quickbase_assigned_order_data A where  A.`ITEM-STATUS` = "shipped"
and a.`SHIPMENT-ID` is null) B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and A. `ORDER-ID` = B.`AMAZON-ORDER-ID` and A.SKU = B.SKU
and `TRANSACTION-TYPE` ="Order") B using(id)
set A.`SHIPMENT-ID` = B.`SHIPMENT-ID` , A.fulfilled_Quantity = B.Quantity;

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

 
-- select * from ygb_quickbase_assigned_order_data where date(`PURCHASE-DATE`) > "2023-01-01" and `ORDER-STATUS` = "Shipped" and `SHIPMENT-ID` is not null;
-- select * from quickbase_settlement_order_data where `ORDER-ID`= "111-0096667-7254667" and "X73686JgL";


select A.*, B.FBA_Fee, B.Commission, B.Principal from ygb_quickbase_assigned_order_data A 
left join combined_quickbase_settlement_order_data B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and  A.`AMAZON-ORDER-ID` = B.`ORDER-ID` 
and A.sku = B.sku and A.`ORDER-STATUS` = B.`TRANSACTION-TYPE` and  A.`SHIPMENT-ID` = B.Group_ID and A.ranking = B.ranking
where date(`PURCHASE-DATE`) > "2023-01-01"
and `ORDER-STATUS` = "Shipped"
and `SHIPMENT-ID` is not null;


-- select * from ygb_quickbase_assigned_return_data  where date(`PURCHASE-DATE`) > "2023-01-01";

-- select * from ygb_quickbase_assigned_order_data where date(`PURCHASE-DATE`) > "2023-01-01";
-- select * from combined_quickbase_settlement_order_data limit 1000;
-- select * from ygb_quickbase_return_data where `ORDER-ID` = "114-7480084-4368216";
-- select * from ygb_quickbase_settlement_returns  where `ORDER-ID` = "114-7480084-4368216";
-- select * from ygb_quickbase_settlement_returns;

-- set @order_id:= "112-3801081-2581846";
-- select * from all_orders where `Amazon-ORDER-ID` =  @order_id;
-- select * from fba_returns where `ORDER-ID` =@order_id;
-- select * from settlements where `ORDER-ID` =@order_id and `TRANSACTION-TYPE` = "Refund" and `AMOUNT-DESCRIPTION` = "Principal";
-- select * from combined_quickbase_settlement_order_data where `TRANSACTION-TYPE` in ("Refund","Chargeback Refund") and status = "posted"  and `order-id` = @order_id;			


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


