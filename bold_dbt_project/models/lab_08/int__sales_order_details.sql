-- This dbt model creates customer records with cleaned up customer names

SELECT
    soh.salesorderid AS salesorderid
    , soh.purchaseordernumber AS purchaseordernumber
    , soh.status AS status
    , soh.accountnumber AS accountnumber
    , soh.taxamt AS taxamt
    , soh.comment AS comment
    , soh.freight AS freight
    , soh.orderdate AS orderdate
    , soh.shipmethodid AS shipmethodid
    , soh.salespersonid AS salespersonid
    , soh.salesordernumber AS salesordernumber
    , soh.billtoaddressid AS billtoaddressid
    , soh.shipdate AS shipdate
    , soh.shiptoaddressid AS shiptoaddressid
    , soh.revisionnumber AS revisionnumber
    , soh.territoryid AS territoryid
    , soh.duedate AS duedate
    , soh.onlineorderflag AS onlineorderflag
    , soh.modifieddate AS modifieddate
    , cust.customerid AS customerid
    , cust.accountnumber AS customeraccountnumber
    , cust.customername AS customername
    , cust.customertype AS customertype
    , addr.addressid AS addressid
    , addr.city AS city
    , addr.postalcode AS postalcode
    , addr.addressline2 AS addressline2
    , addr.addressline1 AS addressline1
    , addr.stateprovinceid AS stateprovinceid
    , addr.statecode AS statecode
    , addr.countrycode AS countrycode
    , sod.salesorderdetailid AS salesorderdetailid
    , sod.carriertrackingnumber AS carriertrackingnumber
    , sod.productid AS productid
    , sod.unitpricediscount AS unitpricediscount
    , sod.linetotal AS linetotal
    , sod.orderqty AS orderqty
    , sod.specialofferid AS specialofferid
    , sod.unitprice AS unitprice
    , prd.style
    , prd.weight
    , prd.size
    , prd.class
    , prd.standardcost
    , prd.name
    , prd.productnumber
    , prd.color
    , prd.productline
    -- STEP 1) Add in the product category and subcategory
    -- STEP 2) Add in the current_timestamp() as a column named last_refreshed
FROM
    {{ ref ( 'base_netsuite__sales_order_header' ) }} soh
INNER JOIN {{ ref ( 'base_netsuite__sales_order_detail' )}} sod ON soh.salesorderid = sod.salesorderid 
INNER JOIN {{ ref ( 'base_netsuite__customer' ) }} cust ON soh.customerid = cust.customerid
INNER JOIN {{ ref ( 'base_netsuite__address' ) }} addr ON soh.shiptoaddressid = addr.addressid
INNER JOIN {{ ref ( 'base_netsuite__product' ) }} prd ON sod.productid = prd.productid