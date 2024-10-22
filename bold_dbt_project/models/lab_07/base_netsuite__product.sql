-- This dbt model creates product records with cleaned up product names and populated product categories

SELECT
    a.productid
    , a.productsubcategoryid
    , a.modifieddate
    , a.sellstartdate
    , a.finishedgoodsflag
    , a.daystomanufacture
    , a.makeflag
    , a.reorderpoint
    , a.style
    , a.weight
    , a.safetystocklevel
    , a.size
    , a.class
    , a.standardcost
    -- STEP 1) Trim the product name to eliminate leading and trailing spaces
    , a.name
    , a.sellenddate
    , a.productnumber
    , a.color
    , a.discontinueddate
    , a.productmodelid
    , a.productline
    , a.rowguid
    , a.sizeunitmeasurecode
    , a.listprice
    , a.weightunitmeasurecode
    , a._fivetran_deleted
    , a._fivetran_synced
    -- If the BOLD Company consisted of multiple sub-companies, we denote which company and system this data came from
    , 'adventureworks' AS source_company_name
    , 'netsuite' AS source_system_name
    -- Generate a system-wide unique key by concatenating the source company name, source system name, and product id, then converting it to an alhpanumeric hash
    , productid AS source_product_id
    , MD5 ( source_company_name || '|' || source_system_name || '|' || source_product_id ) AS product_key
FROM
    {{ source ( 'netsuite' , 'product' ) }} a
-- STEP 2) Join to productsubcategory to add the product subcategory name to the columns above
-- STEP 3) Join to productcategory to add the product category name to the columns above
WHERE
    _fivetran_deleted = FALSE