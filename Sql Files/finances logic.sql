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
create table if not exists finances_transactions_mapping(primary key(Fee_Type, credit_debit))
Select A.Fee_Type, A.credit_debit, 
ifnull(B.`TRANSACTION-TYPE`,"other-transaction") as `TRANSACTION-TYPE`, ifnull(B.`AMOUNT-TYPE`, "other-transaction") as `AMOUNT-TYPE`, 
ifnull(B.`AMOUNT-DESCRIPTION`,A.Fee_Type) as `AMOUNT-DESCRIPTION` from finances_transactions_types A
left join (select distinct `TRANSACTION-TYPE`, `AMOUNT-TYPE`, `AMOUNT-DESCRIPTION` , case when AMOUNT <0 then "Debit" else "Credit" end as  credit_debit from settlements) B
on  A.Fee_Type = B.`AMOUNT-DESCRIPTION` and B.credit_debit = A.credit_debit
group by 
A.Fee_Type, A.credit_debit;



drop table if exists finances_total_amounts;
create table if not exists finances_total_amounts(primary key(account_name, GROUP_ID))
select account_name, GROUP_ID, sum(CURRENCYAMOUNT) as Total_amount, min(POSTEDDATE) as start_date, max(POSTEDDATE) as end_date from finances group by account_name, GROUP_ID;


drop table if exists quickbase_finances_upload;
create table if not exists quickbase_finances_upload
select "Open" as STATUS, A.ACCOUNT_NAME, A.GROUP_ID as  `SETTLEMENT-ID`,CURRENCYCODE as  CURRENCY,
date(start_date) as `SETTLEMENT-START-DATE`,date(end_date) as`SETTLEMENT-END-DATE`,
c.Total_amount as `TOTAL-AMOUNT`, D.`TRANSACTION-TYPE`, D.`AMOUNT-TYPE`, D.`AMOUNT-DESCRIPTION`, 
ifnull(date(POSTEDDATE), date(end_date)) as `POSTED-DATE`, `SELLERSKU` as `SKU`,b.asin, sum(`CURRENCYAMOUNT`)  as AMOUNT 
from finances A
left join ygb_product_account_asin B on a.account_name = b.account_name and a.SELLERSKU = b.sku
left join finances_total_amounts C  on a.account_name = c.account_name and  a.GROUP_ID = c.GROUP_ID
left join finances_transactions_mapping D on 
case when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  = D.fee_type and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = D.credit_debit
group by 
A.ACCOUNT_NAME, A.GROUP_ID, 
date(start_date),  date(end_date),
c.`Total_amount`,d.`TRANSACTION-TYPE`, 
d.`AMOUNT-TYPE`, d.`AMOUNT-DESCRIPTION`, ifnull(date(POSTEDDATE), date(end_date)), `SELLERSKU`, b.asin;



drop table if exists ygb_unique_account_sku;
create table if not exists ygb_unique_account_sku(primary key (account_name, sku))
select * from
(select account_name, sku, asin, row_number() over (partition by account_name, sku order by asin desc) as ranking
from product_data) A 
where ranking = 1;

Drop table if exists quickbase_settlement_order_data;
create table if not exists quickbase_settlement_order_data(primary key(ACCOUNT_NAME, `ORDER-ID`, SKU))
select "POSTED" AS STATUS, ACCOUNT_NAME, 
date(`SETTLEMENT-START-DATE`) as Start_Date, 
date(`SETTLEMENT-END-DATE`) as End_Date, 
`POSTED-DATE`, `SETTLEMENT-ID` , `ORDER-ID`, SKU, ASIN,  `AMOUNT-DESCRIPTION` , 
sum(case when  `AMOUNT-DESCRIPTION` = "FBAPerUnitFulfillmentFee" then AMOUNT else null end) as FBA_Fee,
sum(case when  `AMOUNT-DESCRIPTION` = "Commission" then AMOUNT else null end) as Commission,
sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then AMOUNT else null end) as Principal                
from settlements A
left join ygb_unique_account_sku B using(ACCOUNT_NAME, sku)
where  `SETTLEMENT-START-DATE` >= "2022-01-01"
and `AMOUNT-DESCRIPTION` in ("FBAPerUnitFulfillmentFee", "Commission", "Principal")
group by  ACCOUNT_NAME, `ORDER-ID`, SKU;



Drop table if exists quickbase_finance_order_data;
create table if not exists quickbase_finance_order_data(primary key(ACCOUNT_NAME, `ORDER-ID`, SKU))
select STATUS, ACCOUNT_NAME,
 `SETTLEMENT-START-DATE` as  Start_Date,
 `SETTLEMENT-END-DATE` as  End_Date,
 POSTEDDATE as  `POSTED-DATE`, `SETTLEMENT-ID` , `ORDER-ID`, SKU, ASIN, `AMOUNT-DESCRIPTION` , 
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "FBAPerUnitFulfillmentFee" then AMOUNT else null end) ,0) as FBA_Fee,
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "Commission" then AMOUNT else null end),0) as Commission,
ifnull(sum(case when  `AMOUNT-DESCRIPTION` = "Principal" then AMOUNT else null end) ,0) as Principal      
FROM
(select "Open" as STATUS, A.ACCOUNT_NAME, date(POSTEDDATE) as POSTEDDATE, A.GROUP_ID as  `SETTLEMENT-ID`,AMAZONORDERID AS  `ORDER-ID`,  CURRENCYCODE as  CURRENCY,
date(start_date) as `SETTLEMENT-START-DATE`,date(end_date) as`SETTLEMENT-END-DATE`,
c.Total_amount as `TOTAL-AMOUNT`, D.`TRANSACTION-TYPE`, D.`AMOUNT-TYPE`, D.`AMOUNT-DESCRIPTION`, 
ifnull(date(POSTEDDATE), date(end_date)) as `POSTED-DATE`, `SELLERSKU` as `SKU`,b.asin, sum(`CURRENCYAMOUNT`)  as AMOUNT 
from finances A
left join ygb_unique_account_sku B on a.account_name = b.account_name and a.SELLERSKU = b.sku
left join finances_total_amounts C  on a.account_name = c.account_name and  a.GROUP_ID = c.GROUP_ID
left join finances_transactions_mapping D on 
case when ADJUSTMENTTYPE is not null then ADJUSTMENTTYPE
when CHARGETYPE is not null then CHARGETYPE
when FEETYPE is not null then FEETYPE
when PROMOTIONTYPE is not null then PROMOTIONTYPE
end  = D.fee_type and case when CURRENCYAMOUNT <0 then "Debit" else "Credit" end  = D.credit_debit
where `AMOUNT-DESCRIPTION` in ("FBAPerUnitFulfillmentFee", "Commission", "Principal")
group by 
A.ACCOUNT_NAME, A.GROUP_ID, A.AMAZONORDERID, 
date(start_date),  date(end_date),
c.`Total_amount`,d.`TRANSACTION-TYPE`, 
d.`AMOUNT-TYPE`, d.`AMOUNT-DESCRIPTION`, ifnull(date(POSTEDDATE), date(end_date)), `SELLERSKU`, b.asin) a
group by  ACCOUNT_NAME,  `ORDER-ID`, SKU;



