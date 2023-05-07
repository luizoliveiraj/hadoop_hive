CREATE EXTERNAL TABLE customer_address (
  customerid int,
  qddressid int,
  rowguid string,
  modifieddate date
)
partitioned by(modifieddate)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/external_databases/data_adventure/customeraddress'
tblproperties("skip.header.line.count"="1");