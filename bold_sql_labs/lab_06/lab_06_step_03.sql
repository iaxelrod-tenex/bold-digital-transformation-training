-- Run a query that returns the top 10 CUSTOMERID and CUSTOMERNAME by monthly contract amount

SELECT
    cust.customerid
    , cust.customername
    , SUM ( cont.contractmonthlyamount ) AS _sum_contractmonthlyamount
FROM
    source.netsuite.customer cust
JOIN source.netsuite.contract cont ON cust.customerid = cont.customerid
GROUP BY
    cust.customerid
    , cust.customername
ORDER BY
    _sum_contractmonthlyamount DESC
LIMIT 10 ;