-- This dbt model creates product records with cleaned up product names and populated product categories

SELECT
    a.addressid AS addressid
    , a.spatiallocation AS spatiallocation
    , a.city AS city
    , a.postalcode AS postalcode
    , a.addressline2 AS addressline2
    , a.addressline1 AS addressline1
    , a.stateprovinceid AS stateprovinceid
    , a.rowguid AS rowguid
    , a.modifieddate AS modifieddate
    , a._fivetran_deleted AS _fivetran_deleted
    , a._fivetran_synced AS _fivetran_synced
    , a.statecode AS statecode
    , a.countrycode AS countrycode
FROM
    {{ source ( 'netsuite' , 'address' ) }} a
WHERE
    _fivetran_deleted = FALSE