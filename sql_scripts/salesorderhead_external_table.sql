--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.salesorderhead (
  salesorderid STRING,
  revisionnumber STRING,
  orderdate STRING,
  duedate STRING,
  shipdate STRING,
  status STRING,
  salesordernumber STRING,
  customerid STRING,
  shiptoaddressid STRING,
  billtoaddressid STRING,
  shipmethod STRING,
  creditcardapprovalcode STRING,
  subtotal STRING,
  taxamt STRING,
  freight STRING,
  totaldue STRING,
  comment STRING,
  rowguid STRING,
  modifieddate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/salesorderhead'
tblproperties("skip.header.line.count"="1")
;

--create analytics table
CREATE TABLE data_adventure.salesorderhead (
  salesorderid INT,
  revisionnumber INT,
  orderdate TIMESTAMP,
  duedate TIMESTAMP,
  shipdate TIMESTAMP,
  status INT,
  salesordernumber STRING,
  customerid INT,
  shiptoaddressid INT,
  billtoaddressid INT,
  shipmethod STRING,
  creditcardapprovalcode STRING,
  subtotal FLOAT,
  taxamt FLOAT,
  freight FLOAT,
  totaldue FLOAT,
  comment STRING,
  rowguid STRING,
  modifieddate TIMESTAMP
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/salesorderhead';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.salesorderhead
PARTITION (modified_year)
select
sohr.*,
year(sohr.modifieddate) as modified_year
from (
    select
        cast(salesorderid as INT) as salesorderid,
        cast(revisionnumber as INT) as revisionnumber,
        case when 
            orderdate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(orderdate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as orderdate,
        case when 
            duedate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(duedate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as duedate,
        case when 
            shipdate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(shipdate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as shipdate,
        cast(status as INT) as status,
        salesordernumber,
        cast(customerid as INT) as customerid,
        cast(shiptoaddressid as INT) as shiptoaddressid,
        cast(billtoaddressid as INT) as billtoaddressid,
        shipmethod,
        creditcardapprovalcode,
        cast(subtotal as FLOAT) as subtotal,
        cast(taxamt as FLOAT) as taxamt,
        cast(freight as FLOAT) as freight,
        cast(totaldue as FLOAT) as totaldue,
        comment,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.salesorderhead
    ) as sohr;