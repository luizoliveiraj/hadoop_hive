--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.customer_address (
  customerid string,
  addressid string,
  rowguid string,
  modifieddate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/customeraddress'
tblproperties("skip.header.line.count"="1");

--create analytics table
CREATE TABLE data_adventure.customer_address (
  customerid INT,
  addressid INT,
  rowguid STRING,
  modifieddate TIMESTAMP
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/customeraddress';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.customer_address
PARTITION (modified_year)
select
car.*,
year(car.modifieddate) as modified_year
from (
    select
        cast(customerid as INT) as customerid,
        cast(addressid as INT) as addressid,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.customer_address
    ) as car;