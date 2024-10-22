-- This dbt model creates product records with cleaned up product names and populated product categories

SELECT
    a.productid as product_id
    , a.productsubcategoryid as product_subcategory_id
    , a.modifieddate as modified_date
    , a.sellstartdate as sell_start_date
    , a.finishedgoodsflag as finished_goods_flag
    , a.daystomanufacture as days_to_manufacture
    , a.makeflag as make_flag
    , a.reorderpoint as reorder_point
    , a.style as style
    , a.weight as weight
    , a.safetystocklevel as stafety_stock_level
    , a.size as size
    , a.class as class
    , a.standardcost as standard_cost
    , a.name as name
    , a.sellenddate as sell_end_date
    , a.productnumber as product_number
    , a.color as color
    , a.discontinueddate as discontinued_date
    , a.productmodelid as product_model_id
    , a.productline as product_line
    , a.rowguid as row_guid
    , a.sizeunitmeasurecode as size_unit_measure_code
    , a.listprice as list_price
    , a.weightunitmeasurecode as weight_unit_measure_code
    , a._fivetran_deleted as _fivetran_deleted
    , a._fivetran_synced as _fivetran_synced
    -- If the BOLD Company consisted of multiple sub-companies, we denote which company and system this data came from
    , 'adventureworks' AS source_company_name
    , 'netsuite' AS source_system_name
    -- Generate a system-wide unique key by concatenating the source company name, source system name, and product id, then converting it to an alhpanumeric hash
    , product_id AS source_product_id
    , MD5 ( source_company_name || '|' || source_system_name || '|' || source_product_id ) AS product_key
FROM
    {{ source ( 'netsuite' , 'product' ) }} a
-- STEP 1) Join to productsubcategory to add the product subcategory name to the columns above
-- STEP 2) Join to productcategory to add the product category name to the columns above
WHERE
    _fivetran_deleted = FALSE