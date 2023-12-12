use ybg_group_amazon_seller_central;

set @start_date:= "2022-11-01";
update ygb_quickbase_po_data set Eta = "2023-10-04" where PO_Status= "Completed" and ETA = "";


drop table if exists ygb_quickbase_active_sku_list;
create table if not exists ygb_quickbase_active_sku_list(primary key(account_name, sku))
select  account_name, SKU, ASIN from product_data
where sku in (select distinct sku from ygb_quickbase_po_data where ETA >= @start_date)
group by  account_name, SKU;



drop table if exists ygb_inventory_ledger_summary;
create table if not exists ygb_inventory_ledger_summary(PRIMARY KEY(ACCOUNT_NAME, MSKU, DATE, DISPOSITION))
select A.ACCOUNT_NAME, DATE, A.ASIN, MSKU, DISPOSITION, SUM(`STARTING WAREHOUSE BALANCE`) AS `STARTING WAREHOUSE BALANCE`,
SUM(RECEIPTS) AS RECEIPTS, 
SUM(`CUSTOMER SHIPMENTS`) as `CUSTOMER SHIPMENTS`, 
sum(`CUSTOMER RETURNS`) as `CUSTOMER RETURNS`, 
sum(`VENDOR RETURNS`) as `VENDOR RETURNS`, 
sum(`FOUND`) as `FOUND`, 
sum(`LOST`) as `LOST`, 
sum(`DAMAGED`) as `DAMAGED`, 
sum(`DISPOSED`) as `DISPOSED`, 
sum(`OTHER EVENTS`) as `OTHER EVENTS`, 
sum(`UNKNOWN EVENTS`) as `UNKNOWN EVENTS`, 
sum(`ENDING WAREHOUSE BALANCE`) as `ENDING WAREHOUSE BALANCE`
from ledger_summary_view A
inner join ygb_quickbase_active_sku_list B on A.account_name = B.account_name  and a.msku = b.sku
where date >=  @start_date 
group by ACCOUNT_NAME, MSKU, DATE, DISPOSITION;


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
select A.ACCOUNT_NAME, `AMAZON-ORDER-ID`, date(`PURCHASE-DATE`) as date, A.SKU, A.asin, `ORDER-STATUS`, `ITEM-STATUS`, sum(-QUANTITY) as QUANTITY
from all_orders A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where  `PURCHASE-DATE` >=  @start_date and  `PURCHASE-DATE` <= @max_date and `AMAZON-ORDER-ID` not like 'S%'
and `ORDER-STATUS` not in ("Cancelled")
and `ITEM-STATUS` not in ("unshipped", "Cancelled")
group by  A.ACCOUNT_NAME, `AMAZON-ORDER-ID`, A.SKU;


drop table if exists ygb_inventory_ledger_returns;
create table if not exists ygb_inventory_ledger_returns(primary key(ACCOUNT_NAME, `ORDER-ID`, SKU , date))
select A.ACCOUNT_NAME, `ORDER-ID`, date(`RETURN-DATE`) as date , A.SKU, A.ASIN, "Return", STATUS, sum(QUANTITY) as QUANTITY
from fba_returns A 
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where status != "REIMBURSED"
and  `RETURN-DATE` >=  @start_date
group by  ACCOUNT_NAME, `ORDER-ID`, date, SKU;


drop table if exists ygb_inventory_ledger_receipts;
create table if not exists ygb_inventory_ledger_receipts
select B.*, ifnull(lag(Rolling_Qty, 1) over(partition by Account_Name, sku order by id),0) as Last_Rolling_qty from
(select A.*, sum(QTY) over(partition by Account_Name, sku order by id) as Rolling_Qty from
(select row_number() over (partition by A.Account_Name, A.sku order by  A.Account_Name, A.sku, ETA) as ID, A.Account_Name,Record_ID_Num, FBA_Shipment_ID,ETA, A.sku, a.asin, "Received",  PO_Status, QTY  ,
Unit_Price, Duties / QTY as Duties, Customs_Fees / QTY as Customs_Fees, Demmurage / QTY as Demmurage, Container_Cost/ QTY as Container_Cost, Trucking_Cost / QTY as Trucking_Cost

from ygb_quickbase_po_data A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where  PO_Status = "Completed" and FBA_Shipment_ID != "" and ETA >= @start_date) A) B;


-- drop table if exists ygb_inventory_ledger_removals;
-- create table if not exists ygb_inventory_ledger_removals
-- select a.Account_Name, `ORDER-ID`, date(`REQUEST-DATE`) as date, a.SKU,B.ASIN as asin, `ORDER-TYPE`, `ORDER-STATUS`, -`SHIPPED-QUANTITY`
-- from fba_removal_order_detail A
-- inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
-- where date(`REQUEST-DATE`) >=  @start_date
-- and `ORDER-STATUS` in ("Completed")
-- and `SHIPPED-QUANTITY` != 0
-- ;


drop table if exists ygb_inventory_ledger_removals;
create table if not exists ygb_inventory_ledger_removals
select a.Account_Name, `ORDER-ID`, date(`REQUEST-DATE`) as date, a.SKU,B.ASIN as asin, 
case when `REMOVAL-ORDER-TYPE` = "Return" then "Vendor Removal" else `REMOVAL-ORDER-TYPE` end as `REMOVAL-ORDER-TYPE`, `DISPOSITION`, -`SHIPPED-QUANTITY`
from fba_removal_shipment_detail A
inner join ygb_quickbase_active_sku_list B on A.account_name = b.account_name  and a.sku = b.sku
where date(`REQUEST-DATE`) >=  @start_date
and `SHIPPED-QUANTITY` != 0;



drop table if exists ygb_inventory_ledger_adjustments;
create table if not exists ygb_inventory_ledger_adjustments
select a.ACCOUNT_NAME,`REFERENCE ID`, DATE, MSKU, a.ASIN, `EVENT TYPE`, `EVENT TYPE` as status, QUANTITY  
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

drop table if exists ygb_inventory_ledger_assignment;
create table if not exists ygb_inventory_ledger_assignment
select A.*, B.record_id_num,  B.FBA_Shipment_ID, B.Rolling_Qty as PO_Rolling_Qty, B.Last_Rolling_qty as PO_Last_Rolling_qty,
B.Unit_Price, B.Duties, B.Customs_Fees, B.Demmurage, B.Container_Cost, B.Trucking_Cost
from ygb_inventory_ledger_detail A 
left join ygb_inventory_ledger_receipts B on A.Account_Name = B.Account_Name and A.sku = B.sku and A.rolling_qty <= B.rolling_qty and greatest(A.rolling_qty,0) >= B.last_rolling_qty;