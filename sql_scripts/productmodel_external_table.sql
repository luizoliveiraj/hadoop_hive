--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.productmodel (
  productmodelid string,
  catalogdescription string,
  rowguid string,
  modifieddate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/productmodel'
tblproperties("skip.header.line.count"="1");

--create analytics table
CREATE TABLE data_adventure.productmodel (
  productmodelid int,
  catalogdescription string,
  rowguid string,
  modifieddate timestamp
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/productmodel';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.productmodel
PARTITION (modified_year)
select
pmr.*,
year(pmr.modifieddate) as modified_year
from (
    select
        cast(productmodelid as INT) as productmodelid,
        catalogdescription,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.productmodel
    ) as pmr;