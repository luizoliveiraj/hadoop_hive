--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.salesorderdetail (
  salesorderid string,
  salesorderdetailid string,
  orderqty string,
  productid string,
  unitprice string,
  unitpricediscount string,
  linetotal string,
  rowguid string,
  modifieddate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/salesorderdetail'
tblproperties("skip.header.line.count"="1");

--create analytics table
CREATE TABLE data_adventure.salesorderdetail (
  salesorderid int,
  salesorderdetailid int,
  orderqty int,
  productid int,
  unitprice float,
  unitpricediscount float,
  linetotal float,
  rowguid string,
  modifieddate timestamp
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/salesorderdetail';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.salesorderdetail
PARTITION (modified_year)
select
sor.*,
year(sor.modifieddate) as modified_year
from (
    select
        cast(salesorderid as INT) as salesorderid,
        cast(salesorderdetailid as INT) as salesorderdetailid,
        cast(orderqty as INT) as orderqty,
        cast(productid as INT) as productid,
        cast(unitprice as float) as unitprice,
        cast(unitpricediscount as float) as unitpricediscount,
        cast(linetotal as float) as linetotal,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.salesorderdetail
    ) as sor;