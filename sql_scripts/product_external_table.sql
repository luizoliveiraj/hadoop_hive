--create a raw table
CREATE EXTERNAL TABLE raw_data_adventure.product (
  productid string,
  productnumber string,
  color string,
  standardcost string,
  listprice string,
  size string,
  weight string,
  productcateforyid string,
  productmodelid string,
  sellstartdate string,
  sellenddate string,
  discontinueddate string,
  thumbnailphoto string,
  thumbnailphotofilename string,
  rowguid string,
  modifieddate string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/product'
tblproperties("skip.header.line.count"="1");

--create analytics table
CREATE TABLE data_adventure.product (
  productid int,
  productnumber string,
  color string,
  standardcost float,
  listprice float,
  size string,
  weight float,
  productcateforyid int,
  productmodelid int,
  sellstartdate timestamp,
  sellenddate timestamp,
  discontinueddate timestamp,
  thumbnailphoto string,
  thumbnailphotofilename string,
  rowguid string,
  modifieddate timestamp
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/product';

--transform data and insert in analytics table
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.product
PARTITION (modified_year)
select
pr.*,
year(pr.modifieddate) as modified_year
from (
    select
        cast(productid as INT) as productid,
        productnumber,
        color,
        cast(standardcost as float) as standardcost,
        cast(listprice as float) as listprice,
        size,
        cast(weight as float) as weight,
        cast(productcateforyid as INT) as productcateforyid,
        cast(productmodelid as INT) as productmodelid,
        case when 
            sellstartdate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(sellstartdate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as sellstartdate,
        case when 
            sellenddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(sellenddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as sellenddate,
        case when 
            discontinueddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(discontinueddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as discontinueddate,
        thumbnailphoto,
        thumbnailphotofilename,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.product
    ) as pr;