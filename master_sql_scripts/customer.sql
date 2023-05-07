CREATE TABLE data_adventure.customer (
    customerid INT,
    companyname INT,
    salesperson string,
    emailaddress string,
    city string,
    addressline1 string,
    addressline2 string,
    postalcode string
    )
    PARTITIONED BY (modified_year INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION '/databases/data_adventure/customer';

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.sales
PARTITION (modified_year)
select
    c.customerid,
    c.companyname,
    c.salesperson,
    c.emailaddress,
    a.city,
    a.addressline1,
    a.addressline2,
    a.postalcode,
    a.modifieddate as last_changed_address,
    year(a.modifieddate) as modified_year
from data_adventure.customer c
left join data_adventure.customer_address ca
    on c.customerid = ca.customerid
left join data_adventure.address a 
    on a.addressid = ca.addressid;