use ybg_group_amazon_seller_central;

drop table if exists finances_transactions_types;
create table if not exists finances_transactions_types(primary key (Fee_Type, credit_debit))
select * from
(select distinct
case when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
	when CHARGETYPE is not null then CHARGETYPE
    when FEETYPE is not null then FEETYPE
    when PROMOTIONTYPE is not null then PROMOTIONTYPE
end as Fee_Type,
ADJUSTMENTTYPE, CHARGETYPE, FEETYPE, PROMOTIONTYPE,
case when CURRENCYAMOUNT <0 then "debit"
	else "credit" end as credit_debit
from finances) A
where fee_type is not null
group by Fee_Type, credit_debit;



drop table if exists finances_transactions_mapping;
create table if not exists finances_transactions_mapping(primary key(Fee_Type, `TRANSACTION-TYPE`, credit_debit))
Select A.Fee_Type, A.credit_debit, 
ifnull(B.`TRANSACTION-TYPE`,"other-transaction") as `TRANSACTION-TYPE`, 
ifnull(B.`AMOUNT-TYPE`, "other-transaction") as `AMOUNT-TYPE`, 
ifnull(B.`AMOUNT-DESCRIPTION`,A.Fee_Type) as `AMOUNT-DESCRIPTION` from finances_transactions_types A
left join (select distinct `TRANSACTION-TYPE`, `AMOUNT-TYPE`, `AMOUNT-DESCRIPTION` , case when AMOUNT <0 then "Debit" else "Credit" end as  credit_debit from settlements) B
on  A.Fee_Type = B.`AMOUNT-DESCRIPTION` and B.credit_debit = A.credit_debit
group by 
A.Fee_Type, `TRANSACTION-TYPE`,  A.credit_debit;



drop table if exists finances_total_amounts;
create table if not exists finances_total_amounts(primary key(account_name, GROUP_ID))
select account_name, GROUP_ID, sum(CURRENCYAMOUNT) as Total_amount, min(POSTEDDATE) as start_date, max(POSTEDDATE) as end_date from finances group by account_name, GROUP_ID;


drop table if exists quickbase_finances_upload;
create table if not exists quickbase_finances_upload
select "Open" as STATUS, A.ACCOUNT_NAME, A.GROUP_ID as  `SETTLEMENT-ID`,CURRENCYCODE as  CURRENCY,
date(start_date) as `SETTLEMENT-START-DATE`,date(end_date) as`SETTLEMENT-END-DATE`,
c.Total_amount as `TOTAL-AMOUNT`,
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  as `OG_TRANSACTION-TYPE`,
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null and CURRENCYAMOUNT >=0 then "Order"
when CHARGETYPE is not null and CURRENCYAMOUNT <0 then "Refund"
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then "Promotion"
end as OG_Fee_TYPE,
ifnull(D.`TRANSACTION-TYPE`,E.`TRANSACTION-TYPE`) as `TRANSACTION-TYPE`,
ifnull(D.`AMOUNT-TYPE`, E.`AMOUNT-TYPE`) as `AMOUNT-TYPE`, 
ifnull(D.`AMOUNT-DESCRIPTION`, E.`AMOUNT-DESCRIPTION`) as `AMOUNT-DESCRIPTION`,
ifnull(date(POSTEDDATE), date(end_date)) as `POSTED-DATE`, `SELLERSKU` as `SKU`,b.asin, sum(`CURRENCYAMOUNT`)  as AMOUNT 
from finances A
left join ygb_product_account_asin B on a.account_name = b.account_name and a.SELLERSKU = b.sku
left join finances_total_amounts C  on a.account_name = c.account_name and  a.GROUP_ID = c.GROUP_ID
left join finances_transactions_mapping D on 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  = D.fee_type and 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null and CURRENCYAMOUNT >=0 then "Order"
when CHARGETYPE is not null and CURRENCYAMOUNT <0 then "Refund"
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then "Promotion"
end = D.`TRANSACTION-TYPE` 
and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = D.credit_debit
left join finances_transactions_mapping E  on 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  = E.fee_type 
and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = E.credit_debit
group by 
A.ACCOUNT_NAME, A.GROUP_ID, 
date(start_date),  date(end_date),
c.`Total_amount`,
ifnull(d.`TRANSACTION-TYPE`, e.`TRANSACTION-TYPE`),
ifnull(D.`AMOUNT-TYPE`, E.`AMOUNT-TYPE`), 
ifnull(d.`AMOUNT-DESCRIPTION`, e.`AMOUNT-DESCRIPTION`), 
ifnull(date(POSTEDDATE), date(end_date)), `SELLERSKU`, b.asin;





drop table if exists ygb_unique_account_sku;
create table if not exists ygb_unique_account_sku(primary key (account_name, sku))
select * from
(select account_name, sku, asin, row_number() over (partition by account_name, sku order by asin desc) as ranking
from product_data) A 
where ranking = 1;

Drop table if exists quickbase_settlement_order_data;
create table if not exists quickbase_settlement_order_data
select 
"POSTED" AS STATUS, ACCOUNT_NAME, 
date(`SETTLEMENT-START-DATE`) as Start_Date, 
date(`SETTLEMENT-END-DATE`) as End_Date, 
`POSTED-DATE`, `SETTLEMENT-ID` ,`TRANSACTION-TYPE`, 
ifnull(`SHIPMENT-ID`, `ADJUSTMENT-ID`) as Group_ID, 
 `ORDER-ID`, SKU, ASIN,  `AMOUNT-DESCRIPTION` , 
sum(case when  `AMOUNT-DESCRIPTION` = "FBAPerUnitFulfillmentFee" then AMOUNT else null end) as FBA_Fee,
sum(case when  `AMOUNT-DESCRIPTION` = "Commission" then AMOUNT else null end) as Commission,
sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then AMOUNT else null end) as Principal ,
sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then `QUANTITY-PURCHASED`  else null end) as QUANTITY              
from settlements A
left join ygb_unique_account_sku B using(ACCOUNT_NAME, sku)
where  `SETTLEMENT-START-DATE` >= "2022-01-01"
and (`AMOUNT-DESCRIPTION` in ("FBAPerUnitFulfillmentFee", "Commission", "Principal")
or 
(`TRANSACTION-TYPE` = "Order" and `AMOUNT-DESCRIPTION` = "MarketplaceFacilitatorTax-Principal")
)

group by  ACCOUNT_NAME, `ORDER-ID`, SKU, `TRANSACTION-TYPE`, ifnull(`SHIPMENT-ID`, `ADJUSTMENT-ID`);
-- case when `TRANSACTION-TYPE` in  ("Refund","Chargeback Refund") then id else `SHIPMENT-ID` end ;

create index order_sku on quickbase_settlement_order_data(ACCOUNT_NAME,  `ORDER-ID`, SKU);


Drop table if exists quickbase_finance_order_data;
create table if not exists quickbase_finance_order_data
select STATUS, ACCOUNT_NAME,
 `SETTLEMENT-START-DATE` as  Start_Date,
 `SETTLEMENT-END-DATE` as  End_Date,
 POSTEDDATE as  `POSTED-DATE`, `SETTLEMENT-ID` , `TRANSACTION-TYPE`, 
concat("Open_", row_number() over (partition by "")) as Group_ID,
 `ORDER-ID`, SKU, ASIN, `AMOUNT-DESCRIPTION` , 
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "FBAPerUnitFulfillmentFee" then AMOUNT else null end) ,0) as FBA_Fee,
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "Commission" then AMOUNT else null end),0) as Commission,
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then AMOUNT else null end) ,0) as Principal , 
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then QUANTITY else null end) ,0) as QUANTITY      
FROM
(select "Open" as STATUS, A.ACCOUNT_NAME, date(POSTEDDATE) as POSTEDDATE, A.GROUP_ID as  `SETTLEMENT-ID`,AMAZONORDERID AS  `ORDER-ID`,  CURRENCYCODE as  CURRENCY,
date(start_date) as `SETTLEMENT-START-DATE`,date(end_date) as`SETTLEMENT-END-DATE`,
c.Total_amount as `TOTAL-AMOUNT`,
ifnull(D.`TRANSACTION-TYPE`,E.`TRANSACTION-TYPE`) as `TRANSACTION-TYPE`,
ifnull(D.`AMOUNT-TYPE`, E.`AMOUNT-TYPE`) as `AMOUNT-TYPE`,
ifnull(D.`AMOUNT-DESCRIPTION`, E.`AMOUNT-DESCRIPTION`) as `AMOUNT-DESCRIPTION`, 
ifnull(date(POSTEDDATE), date(end_date)) as `POSTED-DATE`, `SELLERSKU` as `SKU`,b.asin, sum(`CURRENCYAMOUNT`)  as AMOUNT , sum(QUANTITY) as QUANTITY
from finances A
left join ygb_unique_account_sku B on a.account_name = b.account_name and a.SELLERSKU = b.sku
left join finances_total_amounts C  on a.account_name = c.account_name and  a.GROUP_ID = c.GROUP_ID
left join finances_transactions_mapping D on 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end = D.fee_type and 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null and CURRENCYAMOUNT >=0 then "Order"
when CHARGETYPE is not null and CURRENCYAMOUNT <0 then "Chargeback Refund"
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then "Promotion"
end = D.`TRANSACTION-TYPE` 
and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = D.credit_debit
left join (SELECT fee_type, credit_debit, `TRANSACTION-TYPE`, `AMOUNT-TYPE`, `AMOUNT-DESCRIPTION` 
			FROM finances_transactions_mapping WHERE `TRANSACTION-TYPE` NOT IN ("Order", "Refund") 
            GROUP BY fee_type, credit_debit) E  on 
case 
when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  = E.fee_type 
and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = E.credit_debit
where ifnull(D.`AMOUNT-DESCRIPTION`, E.`AMOUNT-DESCRIPTION`) in ("FBAPerUnitFulfillmentFee", "Commission", "Principal")
group by 
A.ACCOUNT_NAME, A.GROUP_ID, A.AMAZONORDERID, 
date(start_date),  date(end_date),
c.`Total_amount`,

ifnull(D.`TRANSACTION-TYPE`,E.`TRANSACTION-TYPE`),
ifnull(D.`AMOUNT-TYPE`, E.`AMOUNT-TYPE`), 
ifnull(D.`AMOUNT-DESCRIPTION`, E.`AMOUNT-DESCRIPTION`), 
ifnull(date(POSTEDDATE), date(end_date)), `SELLERSKU`, b.asin) a
group by  ACCOUNT_NAME,  `ORDER-ID`, SKU, `TRANSACTION-TYPE`;


drop table if exists combined_quickbase_settlement_order_data;
create table if not exists combined_quickbase_settlement_order_data
select row_number() over (partition by ACCOUNT_NAME, `ORDER-ID`, sku, `TRANSACTION-TYPE`, Group_ID order by ACCOUNT_NAME, `ORDER-ID`, sku, `TRANSACTION-TYPE`) as ranking, A.* from
(select * from quickbase_settlement_order_data
union
select * from quickbase_finance_order_data) A;

update combined_quickbase_settlement_order_data set `TRANSACTION-TYPE` = "Shipped" where  `TRANSACTION-TYPE`  = "Order";
update combined_quickbase_settlement_order_data set `TRANSACTION-TYPE` = "Return" where  `TRANSACTION-TYPE`  = "Refund";
update combined_quickbase_settlement_order_data set `TRANSACTION-TYPE` = "Return" where  `TRANSACTION-TYPE`  = "Chargeback Refund";

update combined_quickbase_settlement_order_data set FBA_Fee = 0.00 where FBA_Fee is null;
update combined_quickbase_settlement_order_data set Commission = 0.00 where Commission is null;
update combined_quickbase_settlement_order_data set Principal = 0.00 where Principal is null;



-- -----------------------------------------------------------------------------------
-- ORDERS ----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------------

drop table if exists ygb_quickbase_order_data;
create table if not exists ygb_quickbase_order_data (primary key( ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ORDER-STATUS`,`ITEM-PRICE`, Ranking))
select
row_number() over (partition by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS` order by A.ACCOUNT_NAME,  `AMAZON-ORDER-ID`, A.SKU, `ORDER-STATUS`) as Ranking,
A.ACCOUNT_NAME, sum(QUANTITY) as QUANTITY, `PURCHASE-DATE`,`ITEM-PRICE` , ifnull(round(`ITEM-PRICE` / QUANTITY,2),`ITEM-PRICE` ) as `UNIT-PRICE`, A.ASIN, `AMAZON-ORDER-ID`, `MERCHANT-ORDER-ID`,
`ORDER-STATUS`, `FULFILLMENT-CHANNEL`, `SALES-CHANNEL`,`ORDER-CHANNEL`, `SHIP-SERVICE-LEVEL`, `PRODUCT-NAME`,
A.SKU, `ITEM-STATUS`,CURRENCY,`ITEM-TAX`, `SHIPPING-PRICE`, `SHIPPING-TAX`, `GIFT-WRAP-PRICE`, `GIFT-WRAP-TAX`,
`ITEM-PROMOTION-DISCOUNT`, `SHIP-PROMOTION-DISCOUNT`, `SHIP-CITY`, `SHIP-STATE`, `SHIP-POSTAL-CODE`,
`SHIP-COUNTRY`, `PROMOTION-IDS`, `IS-BUSINESS-ORDER`, `PURCHASE-ORDER-NUMBER`, `PRICE-DESIGNATION`,
`IS-TRANSPARENCY`, `SIGNATURE-CONFIRMATION-RECOMMENDED`
from all_orders A
where  `PURCHASE-DATE` >= "2021-01-01" 
-- and `AMAZON-ORDER-ID`= "111-2301076-9064201"
group by  ACCOUNT_NAME,  `AMAZON-ORDER-ID`, SKU, `ORDER-STATUS`, ifnull(round(`ITEM-PRICE` / QUANTITY,2),`ITEM-PRICE` ), 
`ORDER-STATUS`, `ITEM-STATUS`;


DROP TABLE if exists ygb_quickbase_order_unit_price;
create table if not exists ygb_quickbase_order_unit_price(primary key( ACCOUNT_NAME, `ORDER-ID`, sku))
select ACCOUNT_NAME, `AMAZON-ORDER-ID` AS `ORDER-ID`, sku, round(SUM(QUANTITY * `UNIT-PRICE`) / QUANTITY,2) AS  `UNIT-PRICE`, SUM(QUANTITY) AS QUANTITY, COUNT(*) AS COUNT
FROM ygb_quickbase_order_data WHERE `ORDER-STATUS` not in ("Cancelled") GROUP BY ACCOUNT_NAME, `AMAZON-ORDER-ID`, sku;


drop table if exists ygb_quickbase_number_table;
create table if not exists ygb_quickbase_number_table(
number int auto_increment primary key
);

insert into ygb_quickbase_number_table values
(1), (2), (3), (4), (5), (6), (7), (8), (9),
(10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
(20), (21), (22), (23), (24), (25), (26), (27), (28), (29),
(30), (31), (32), (33), (34), (35), (36), (37), (38), (39),
(40), (41), (42), (43), (44), (45), (46), (47), (48), (49),
(50), (51), (52), (53), (54), (55), (56), (57), (58), (59),
(60), (61), (62), (63), (64), (65), (66), (67), (68), (69),
(70), (71), (72), (73), (74), (75), (76), (77), (78), (79),
(80), (81), (82), (83), (84), (85), (86), (87), (88), (89),
(90), (91), (92), (93), (94), (95), (96), (97), (98), (99),
(100);


drop table if exists ygb_quickbase_settlement_returns_setup;
create table if not exists ygb_quickbase_settlement_returns_setup
select a.*, b.QUANTITY as Settlement_Quantity, B.`UNIT-PRICE`, round(ifnull(greatest(-a.Principal / B.`UNIT-PRICE`,1),1),0) AS Order_Line_Units  from
(select 
ranking, STATUS, ACCOUNT_NAME, Start_Date, End_Date, `POSTED-DATE`, `SETTLEMENT-ID`, `TRANSACTION-TYPE`, `Group_ID`, `ORDER-ID`, `SKU`, `ASIN`, `AMOUNT-DESCRIPTION`, `FBA_Fee`, `Commission`, Principal, QUANTITY
from combined_quickbase_settlement_order_data  where `TRANSACTION-TYPE` in ("Return")) A
left join 
ygb_quickbase_order_unit_price B USING(ACCOUNT_NAME,  `ORDER-ID`, sku);

drop table if exists ygb_quickbase_settlement_returns;
create table if not exists ygb_quickbase_settlement_returns(primary key(ACCOUNT_NAME, `ORDER-ID`, SKU, new_ranking))
select *, row_number() over (partition by ACCOUNT_NAME, `ORDER-ID`, SKU order by ACCOUNT_NAME, `ORDER-ID`, SKU) as new_ranking from ygb_quickbase_settlement_returns_setup A
left join 
ygb_quickbase_number_table B
on A.Order_Line_Units >= B.number;

create index unique_detail on ygb_quickbase_settlement_returns (ACCOUNT_NAME, `ORDER-ID`, SKU);
