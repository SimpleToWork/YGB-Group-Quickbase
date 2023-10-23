

set @max_date:=(select max(date) from ledger_summary_view);

select * from all_inventory;
select * from ledger_summary_view where DATE = @max_date;
select * from ledger_detail_view where MSKU = "3W-9BHO-WF4J" and `EVENT TYPE` = "CustomerReturns";
select * from fba_returns where sku = "3W-9BHO-WF4J";

drop table if exists ygb_inventory_ledger;
create table if not exists ygb_inventory_ledger
select * from  ledger_detail_view where MSKU = "3W-9BHO-WF4J" and `EVENT TYPE` = "CustomerReturns";

Alter table ygb_inventory_ledger 
add column `ID` int unique auto_increment first ,
add column `Ranking` int,
add column `ORDER-ID` varchar(65),
add column `RETURN-DATE` datetime,
add column `STATUS`  varchar(65),
add column `FULFILLMENT-CENTER-ID` varchar(10); 

update ygb_inventory_ledger A inner join (
select *, row_number() over (partition by ACCOUNT_NAME, `EVENT TYPE`, MSKU, `FULFILLMENT CENTER`, DISPOSITION order by  ACCOUNT_NAME, `EVENT TYPE`, `FULFILLMENT CENTER`,DISPOSITION,  MSKU, date desc) as ranked
from ygb_inventory_ledger
) B using(id)
set a.ranking = b.ranked;

update ygb_inventory_ledger A inner join 
(select *, row_number() over (partition by ACCOUNT_NAME, SKU, `FULFILLMENT-CENTER-ID`, `DETAILED-DISPOSITION` order by ACCOUNT_NAME, SKU,`FULFILLMENT-CENTER-ID`,`DETAILED-DISPOSITION`, `RETURN-DATE` desc) as ranking 
from fba_returns where  sku = "3W-9BHO-WF4J") B on 
A.ACCOUNT_NAME = B.ACCOUNT_NAME and A.MSKU = B.SKU and A.`FULFILLMENT CENTER` = B. `FULFILLMENT-CENTER-ID`  and A.ranking = B.ranking and  a.DISPOSITION = b.`DETAILED-DISPOSITION` 
set a.`ORDER-ID` = b.`ORDER-ID`,
a.`RETURN-DATE` = b.`RETURN-DATE`,
A.`STATUS` = B.`STATUS`,
A.`FULFILLMENT-CENTER-ID` = B.`FULFILLMENT-CENTER-ID`;

select * from ygb_inventory_ledger where  `EVENT TYPE` = "CustomerReturns" and `FULFILLMENT CENTER` = "IND8" and DISPOSITION = "CUSTOMER_DAMAGED";
select * from ygb_quickbase_po_data;
select * from all_orders  where sku = "3W-9BHO-WF4J" ;
select * from ygb_inventory_ledger where  `EVENT TYPE` = "CustomerReturns";
select *, row_number() over (partition by ACCOUNT_NAME, SKU order by ACCOUNT_NAME, SKU, `RETURN-DATE` desc) as ranked from fba_returns where sku = "3W-9BHO-WF4J";



