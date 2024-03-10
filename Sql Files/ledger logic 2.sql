drop table if exists ygb_quickbase_active_sku_list;
create table if not exists ygb_quickbase_active_sku_list(primary key(account_name, sku))
select distinct account_name, SKU from product_data
where sku in ("B2-Wood-652",  "3W-9BHO-WF4J");

drop table if exists ygb_inventory_ledger_summary;
create table if not exists ygb_inventory_ledger_summary(PRIMARY KEY(ACCOUNT_NAME, MSKU, DATE, DISPOSITION))
select A.ACCOUNT_NAME, DATE, ASIN, MSKU, DISPOSITION, `STARTING WAREHOUSE BALANCE`, RECEIPTS, `CUSTOMER SHIPMENTS`, `CUSTOMER RETURNS`, `VENDOR RETURNS`, `ENDING WAREHOUSE BALANCE`
from ledger_summary_view A
inner join ygb_quickbase_active_sku_list B on A.account_name = B.account_name  and a.msku = b.sku
where date >=  @start_date ;

SET @MAX_DATE = (SELECT MAX(DATE) FROM ledger_summary_view  );

drop table if exists ygb_inventory_ledger;
create table if not exists ygb_inventory_ledger(primary key(ACCOUNT_NAME, MSKU, `EVENT TYPE`, DISPOSITION, date, ranking))
SELECT 
row_number() over (partition by "" order by DATE desc) as ID,
A.ACCOUNT_NAME, A.DATE, A.MSKU, A.`EVENT TYPE`, a.QUANTITY, A.DISPOSITION,  
B.`ENDING WAREHOUSE BALANCE` ,  row_number() over (partition by ACCOUNT_NAME, MSKU,  `DISPOSITION`, DATE order by ACCOUNT_NAME, MSKU, `DISPOSITION`, `DATE` ASC) as ranking
FROM ledger_detail_view a 
LEFT JOIN ygb_inventory_ledger_summary b USING(ACCOUNT_NAME, MSKU, DATE, DISPOSITION) 
inner join ygb_quickbase_active_sku_list C on A.account_name = c.account_name  and a.msku = c.sku
WHERE date >=  @start_date and date <=  @MAX_DATE and `EVENT TYPE` not in ("WhseTransfers")
-- and `EVENT TYPE` = "Shipments"
-- AND disposition = "SELLABLE"
order by date desc;



drop table if exists ygb_inventory_ledger_order_setup;
create table if not exists ygb_inventory_ledger_order_setup(primary key(ACCOUNT_NAME, `AMAZON-ORDER-ID`, sku))
select A.ACCOUNT_NAME, `AMAZON-ORDER-ID`, date(`PURCHASE-DATE`) as date, A.SKU, asin, `ORDER-STATUS`, `ITEM-STATUS`, sum(-QUANTITY) as QUANTITY
from all_orders A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where  `PURCHASE-DATE` >=  @start_date and  `PURCHASE-DATE` <= @max_date and `AMAZON-ORDER-ID` not like 'S%'
and `ORDER-STATUS` not in ("Cancelled")
and `ITEM-STATUS` not in ("unshipped", "Cancelled")
group by  A.ACCOUNT_NAME, `AMAZON-ORDER-ID`, A.SKU;


drop table if exists ygb_inventory_ledger_returns;
create table if not exists ygb_inventory_ledger_returns(primary key(ACCOUNT_NAME, `ORDER-ID`, SKU , date))
select A.ACCOUNT_NAME, `ORDER-ID`, date(`RETURN-DATE`) as date , A.SKU, ASIN, "Return", STATUS, sum(QUANTITY) as QUANTITY
from fba_returns A 
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where status != "REIMBURSED"
and  `RETURN-DATE` >=  @start_date
group by  ACCOUNT_NAME, `ORDER-ID`, date, SKU;


drop table if exists ygb_inventory_ledger_receipts;
create table if not exists ygb_inventory_ledger_receipts
select B.*, ifnull(lag(Rolling_Qty, 1) over(partition by Account_Name, sku order by id),0) as Last_Rolling_qty from
(select A.*, sum(QTY) over(partition by Account_Name, sku order by id) as Rolling_Qty from
(select row_number() over (partition by A.Account_Name, A.sku order by  A.Account_Name, A.sku, ETA) as ID, A.Account_Name,FBA_Shipment_ID,ETA, A.sku, asin, "Received",  PO_Status, QTY  
from ygb_quickbase_po_data A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where  PO_Status = "Completed" and FBA_Shipment_ID != "") A) B;


drop table if exists ygb_inventory_ledger_removals;
create table if not exists ygb_inventory_ledger_removals
select a.Account_Name, `ORDER-ID`, date(`REQUEST-DATE`) as date, a.SKU, "" as asin, "Removal", `ORDER-STATUS`, -`SHIPPED-QUANTITY`
from fba_removal_order_detail A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where date(`REQUEST-DATE`) >=  @start_date
and `ORDER-STATUS` in ("Completed");


drop table if exists ygb_inventory_ledger_adjustments;
create table if not exists ygb_inventory_ledger_adjustments
select a.ACCOUNT_NAME,`REFERENCE ID`, DATE, MSKU, ASIN, `EVENT TYPE`, `EVENT TYPE` as status, QUANTITY  
from ledger_detail_view A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.msku = b.sku
where `EVENT TYPE` = "Adjustments"  and date >=  @start_date;

drop table if exists ygb_inventory_ledger_detail;
create table if not exists ygb_inventory_ledger_detail(primary key(id))
select row_number() over (partition by "" order by  account_name, sku, date asc, `AMAZON-ORDER-ID` ) as ID, A.* from
(select * from ygb_inventory_ledger_order_setup
union
select * from ygb_inventory_ledger_returns 
union 
select * from ygb_inventory_ledger_removals
union
select * from ygb_inventory_ledger_adjustments) A;

Alter table ygb_inventory_ledger_detail 
add column rolling_qty int,
add column last_rolling_qty int;


update ygb_inventory_ledger_detail A inner join
(select *, sum(-quantity) over (partition by account_name, sku order by  account_name, sku, date, `AMAZON-ORDER-ID`) as set_quantity from ygb_inventory_ledger_detail) B using(id)
Set a.rolling_qty = B.set_quantity;

update ygb_inventory_ledger_detail A inner join
(select *, ifnull(lag(rolling_qty, 1) over(partition by account_name, sku order by  account_name, sku, date, `AMAZON-ORDER-ID`),0) as Last_Rolling from ygb_inventory_ledger_detail) B using(id)
Set a.last_rolling_qty = B.Last_Rolling;

select A.*, B.FBA_Shipment_ID, B.Rolling_Qty, B.Last_Rolling_qty
from ygb_inventory_ledger_detail A 
left join ygb_inventory_ledger_receipts B on A.Account_Name = B.Account_Name and A.sku = B.sku and A.rolling_qty <= B.rolling_qty and A.rolling_qty > B.last_rolling_qty;



-- select * from ygb_inventory_ledger_detail;
-- select * from ygb_inventory_ledger_receipts;





-- -- select * from fba_removal_shipment_detail where  sku = "3W-9BHO-WF4J";

-- select *  from 
-- ledger_detail_view a LEFT JOIN ygb_inventory_ledger_summary b USING(ACCOUNT_NAME, MSKU, DATE, DISPOSITION) 
-- WHERE  MSKU = "3W-9BHO-WF4J" and date >= "2022-11-01" and date <=  @MAX_DATE and `EVENT TYPE` not in ("WhseTransfers")
-- and `EVENT TYPE` = "Receipts";



