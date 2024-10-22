-- Query the customer table for a random shippingaddressid

SELECT
    customerid
    , accountnumber
    , customername
    , customertype
    , shippingaddressid
FROM
    source.netsuite.customer
WHERE
    shippingaddressid IS NOT NULL
LIMIT
    5 ;