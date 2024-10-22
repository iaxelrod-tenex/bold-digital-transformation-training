-- Run a query that returns a count of all customers by customer type

SELECT
    customertype AS customer_type
    , count(1) AS customer_type_count
FROM
    source.netsuite.customer cust 
GROUP BY
    customertype ;