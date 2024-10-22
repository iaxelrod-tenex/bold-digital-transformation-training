-- Run a SQL query with a WHERE clause

SELECT
    *
FROM
    source.netsuite.customer
WHERE
    customertype = 'Company'
LIMIT
    200 ;