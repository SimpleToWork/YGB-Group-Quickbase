drop table if exists ygb_quickbase_order_data;
create table if not exists ygb_quickbase_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ORDER-STATUS`,`ITEM-PRICE`, Ranking))
select
row_number() over (partition by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS` order by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`) as Ranking,
A.ACCOUNT_NAME, sum(QUANTITY) as QUANTITY, `PURCHASE-DATE`,ifnull(round(`ITEM-PRICE` / QUANTITY,2),`ITEM-PRICE` ) as `ITEM-PRICE`, A.ASIN, `AMAZON-ORDER-ID`, `MERCHANT-ORDER-ID`,
`ORDER-STATUS`, `FULFILLMENT-CHANNEL`, `SALES-CHANNEL`,`ORDER-CHANNEL`, `SHIP-SERVICE-LEVEL`, `PRODUCT-NAME`,
A.SKU, `ITEM-STATUS`,CURRENCY,`ITEM-TAX`, `SHIPPING-PRICE`, `SHIPPING-TAX`, `GIFT-WRAP-PRICE`, `GIFT-WRAP-TAX`,
`ITEM-PROMOTION-DISCOUNT`, `SHIP-PROMOTION-DISCOUNT`, `SHIP-CITY`, `SHIP-STATE`, `SHIP-POSTAL-CODE`,
`SHIP-COUNTRY`, `PROMOTION-IDS`, `IS-BUSINESS-ORDER`, `PURCHASE-ORDER-NUMBER`, `PRICE-DESIGNATION`,
`IS-TRANSPARENCY`, `SIGNATURE-CONFIRMATION-RECOMMENDED`
-- ifnull(b.STATUS, c.STATUS) as STATUS,  ifnull(B.FBA_Fee, C.FBA_Fee) as FBA_Fee,
-- ifnull(B.Commission, C.Commission) as Commission, ifnull(B.Principal, C.Principal) as Principal
from all_orders A
-- left join quickbase_settlement_order_data B on A.ACCOUNT_NAME = B.ACCOUNT_NAME and A.`AMAZON-ORDER-ID` = B.`ORDER-ID` and A.SKU = b.SKU
-- left join quickbase_finance_order_data C on A.ACCOUNT_NAME = C.ACCOUNT_NAME and A.`AMAZON-ORDER-ID` = C.`ORDER-ID` and A.SKU = C.SKU
where  `PURCHASE-DATE` >= "2023-01-01" 
-- and  `AMAZON-ORDER-ID` =  "111-1457537-2870604"
group by  ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ORDER-STATUS`, ifnull(round(`ITEM-PRICE` / QUANTITY,2),`ITEM-PRICE` ), 
`ORDER-STATUS`, `ITEM-STATUS`
;

create index unique_level on ygb_quickbase_order_data( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`);


drop table if exists ygb_quickbase_fulfilled_order_data;
create table if not exists ygb_quickbase_fulfilled_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`, `SHIPMENT-ID`))
select account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`, "shipped" as `ITEM-STATUS`, round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) as `ITEM-PRICE`, sum(`QUANTITY-SHIPPED`) as Quantity  
from amazon_fulfilled_shipments 
-- where `AMAZON-ORDER-ID` =  "111-1457537-2870604"
group by  account_name, `AMAZON-ORDER-ID`, SKU, `SHIPMENT-ID`,round(`ITEM-PRICE` / `QUANTITY-SHIPPED`,2) ;  


select a.* , b.`SHIPMENT-ID`, b.Quantity AS fulfilled_Quantiy
from ygb_quickbase_order_data A 
left join ygb_quickbase_fulfilled_order_data B USING( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ITEM-STATUS`,`ITEM-PRICE`)
where A.`ITEM-STATUS` = "shipped"
and B.`SHIPMENT-ID` is null;

select * from ygb_quickbase_order_data a where A.`ITEM-STATUS` = "shipped";
            
select * from ygb_quickbase_order_data where `AMAZON-ORDER-ID` = "111-1457537-2870604" 
select * from all_orders where `AMAZON-ORDER-ID` = "111-1457537-2870604" and `ITEM-STATUS` = "Shipped";



select distinct `SHIPMENT-ID` from amazon_fulfilled_shipments where `AMAZON-ORDER-ID` =  "111-1457537-2870604";

select * from amazon_fulfilled_shipments where `AMAZON-ORDER-ID` =  "111-1457537-2870604";

select * from quickbase_settlement_order_data where `ORDER-ID` =  "111-1457537-2870604";



Error Code: 1062. Duplicate entry 'Ygb Group-111-0014988-7593845-3W-9Bho-Wf4J-Shipped' for key 'ygb_quickbase_order_data.PRIMARY'
Error Code: 1062. Duplicate entry 'Ygb Group-111-0187639-2684211-3W-9Bho-Wf4J-Shipped' for key 'ygb_quickbase_order_data.PRIMARY'


             
         --     and `PURCHASE-DATE` < "{next_date_to_recruit}";