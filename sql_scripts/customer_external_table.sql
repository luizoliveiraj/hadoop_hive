SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE EXTERNAL TABLE raw_data_adventure.customer_raw (
  customerid STRING,
  title STRING,
  suffix STRING,
  companyname STRING,
  salesperson STRING,
  emailaddress STRING,
  passwordhash STRING,
  passwordsalt STRING,
  rowguid STRING,
  modifieddate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/customer'
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE data_adventure.customer (
  customerid INT,
  title STRING,
  suffix STRING,
  companyname STRING,
  salesperson STRING,
  emailaddress STRING,
  passwordhash STRING,
  passwordsalt STRING,
  rowguid STRING,
  modified_date TIMESTAMP
)
PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/customer';


;
INSERT OVERWRITE TABLE  data_adventure.customer
PARTITION (modified_year)
select
cr.*,
year(cr.modifieddate) as modified_year
from (
    select
        cast(customerid as INT) as customerid,
        title,
        suffix,
        companyname,
        salesperson,
        emailaddress,
        passwordhash,
        passwordsalt,
        rowguid,
        case when 
            modifieddate is null then to_date('2000-01-01')
            else date_format(from_unixtime(unix_timestamp(modifieddate, 'dd-MM-yyyy')), 'yyyy-MM-dd')
            end as modifieddate
    from raw_data_adventure.customer_raw
    ) as cr;