-- Run a SQL query to select specific columns

SELECT
    customerid
    , accountnumber
    , customername
    , customertype
FROM
    source.netsuite.customer
WHERE
    customertype = 'Company'
LIMIT
    200 ;