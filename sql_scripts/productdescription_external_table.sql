--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.productmodeldescription (
  productdescriptionid string,
  description string,
  rowguid string,
  modifieddate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/productmodeldescription'
tblproperties("skip.header.line.count"="1");

--create analytics table
CREATE TABLE data_adventure.productmodeldescription (
  productdescriptionid int,
  description string,
  rowguid string,
  modifieddate timestamp
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/productmodeldescription';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.productmodeldescription
PARTITION (modified_year)
select
pdr.*,
year(pdr.modifieddate) as modified_year
from (
    select
        cast(productdescriptionid as INT) as productdescriptionid,
        description,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.productmodeldescription
    ) as pdr;