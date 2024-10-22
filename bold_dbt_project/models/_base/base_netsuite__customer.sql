-- This dbt model creates deduplicated customer records with cleaned up customer names

WITH base_data AS (
    SELECT
        *
    FROM
        {{ source ( 'netsuite' , 'customer' ) }}
    
    -- Filter out all records that have been "deleted"
    WHERE
        _fivetran_deleted = FALSE

    -- The following logic will remove any duplicate customers with the same customerid value
    QUALIFY
        ROW_NUMBER() OVER ( PARTITION BY customerid ORDER BY _fivetran_synced DESC ) = 1
)

SELECT
    a.customerid as customer_id
    , a.modifieddate as modified_date
    , a.personid as person_id
    , a.accountnumber as account_number
    , a.companyid as company_id
    , a.rowguid as row_guid
    , a.territoryid as territory_id
    -- The following logic will trim any trailing or leading spaces in the customer name
    , TRIM ( a.customername ) as customer_name
    -- The following logic will convert the customer name to all upper case letters
    , UPPER ( customer_name ) AS customer_name_upper
    , LOWER ( customer_name ) AS customer_name_lower
    , a.customertype as customer_type
    , a.billingaddressid as billing_address_id
    , a.shippingaddressid as shipping_address_id
    , a._fivetran_deleted as _fivetran_deleted
    , a._fivetran_synced as _fivetran_synced
    -- If the BOLD Company consisted of multiple sub-companies, we denote which company and system this data came from
    , 'adventureworks' AS source_company_name
    , 'netsuite' AS source_system_name
    , customer_id AS source_customer_id
    -- Generate a system-wide unique key by concatenating the source company name, source system name, and customer id, then converting it to an alhpanumeric hash
    , MD5 ( source_company_name || '|' || source_system_name || '|' || source_customer_id ) AS customer_key
FROM
    base_data a