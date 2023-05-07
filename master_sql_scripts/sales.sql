CREATE TABLE data_adventure.sales (
    salesorderid INT,
    orderqty INT,
    unitprice FLOAT,
    unitpricediscount FLOAT,
    linetotal FLOAT,
    orderdate TIMESTAMP,
    duedate TIMESTAMP,
    shipdate TIMESTAMP,
    subtotal FLOAT,
    taxamt FLOAT,
    freight FLOAT,
    totaldue FLOAT,
    customerid INT,
    companyname string,
    productid INT,
    color STRING,
    standardcost FLOAT,
    listprice FLOAT,
    size STRING,
    weight float,
    sellstartdate TIMESTAMP,
    sellenddate TIMESTAMP,
    discontinueddate TIMESTAMP
) PARTITIONED BY (modified_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/databases/data_adventure/sales'
;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE  data_adventure.sales
PARTITION (modified_year)
select
    so.salesorderid,
    so.orderqty,
    so.unitprice,
    so.unitpricediscount,
    so.linetotal,
    soh.orderdate,
    soh.duedate,
    soh.shipdate,
    soh.subtotal,
    soh.taxamt,
    soh.freight,
    soh.totaldue,
    c.customerid,
    c.companyname,
    p.productid,
    p.color,
    p.standardcost,
    p.listprice,
    p.size,
    p.weight,
    p.sellstartdate,
    p.sellenddate,
    p.discontinueddate,
    year(p.modifieddate) as modified_year
from data_adventure.salesorderdetail so
    left join data_adventure.salesorderhead soh 
        on soh.salesorderid = so.salesorderid
    left join data_adventure.customer c
        on c.customerid = soh.customerid
    left join data_adventure.product p
        on so.productid = p.productid;