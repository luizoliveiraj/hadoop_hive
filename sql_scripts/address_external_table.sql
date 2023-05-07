CREATE EXTERNAL TABLE raw_data_adventure.address_raw (
  addressid STRING,
  addressline1 STRING,
  addressline2 STRING,
  city STRING,
  postalcode string,
  rowguid string,
  modifieddate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/address'
tblproperties("skip.header.line.count"="1");

CREATE TABLE data_adventure.address (
  addressid INT,
  addressline1 STRING,
  addressline2 STRING,
  city STRING,
  postalcode string,
  rowguid string,
  modifieddate TIMESTAMP
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/address';

INSERT OVERWRITE TABLE  data_adventure.address
PARTITION (modified_year)
select
ar.*,
year(ar.modifieddate) as modified_year
from (
    select
        cast(addressid as INT) as addressid,
        addressline1,
        addressline2,
        city,
        postalcode,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.address_raw
    ) as ar;