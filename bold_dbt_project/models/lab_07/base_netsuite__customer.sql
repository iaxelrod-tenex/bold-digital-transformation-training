-- This dbt model creates customer records with cleaned up customer names

SELECT
    a.customerid
    , a.modifieddate
    , a.personid
    , a.accountnumber
    , a.companyid
    , a.rowguid
    , a.territoryid
    -- The following logic will trim any trailing or leading spaces in the customer name
    , TRIM ( a.customername ) AS customername
    -- The following logic will convert the customer name to all upper case letters
    , UPPER ( a.customername ) AS customername_upper
    , LOWER ( a.customername ) AS customername_lower
    , a.customertype
    , a.billingaddressid
    , a.shippingaddressid
    , a._fivetran_deleted
    , a._fivetran_synced
    -- If the BOLD Company consisted of multiple sub-companies, we denote which company and system this data came from
    , 'adventureworks' AS source_company_name
    , 'netsuite' AS source_system_name
    -- Generate a system-wide unique key by concatenating the source company name, source system name, and customer id, then converting it to an alhpanumeric hash
    , customerid AS source_customer_id
    , MD5 ( source_company_name || '|' || source_system_name || '|' || source_customer_id ) AS customer_key
FROM
    {{ source ( 'netsuite' , 'customer' ) }} a
WHERE
    _fivetran_deleted = FALSE