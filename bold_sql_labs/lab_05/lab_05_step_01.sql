-- Run a query that inner joins CUSTOMER to ADDRESS using the SHIPPINGADDRESSID column

SELECT
    cust.customerid
    , cust.accountnumber
    , cust.customername
    , cust.customertype
    , cust.billingaddressid
    , addr.*
FROM
    source.netsuite.customer cust
INNER JOIN source.netsuite.address addr ON cust.shippingaddressid = addr.addressid 
LIMIT 200; 