-- Run a query that returns the top 10 country and state codes by total customer count, including customers with no address on file

SELECT
    addr.countrycode
    , addr.statecode
    , COUNT(1) statecode_count
FROM
    source.netsuite.customer cust
LEFT JOIN source.netsuite.address addr ON cust.shippingaddressid = addr.addressid 
GROUP BY
    addr.countrycode
    , addr.statecode
ORDER BY
    statecode_count DESC
LIMIT 10 ;