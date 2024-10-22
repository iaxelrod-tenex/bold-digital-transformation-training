-- Query the Snowflake metadata to find tables the shippingaddressid column might join to

SELECT
    table_schema
    , table_name
    , column_name
    , data_type
FROM
    source.information_schema.columns
WHERE
    table_schema = 'NETSUITE'
AND column_name ILIKE '%addressid%'
ORDER BY
    table_name
    , column_name;