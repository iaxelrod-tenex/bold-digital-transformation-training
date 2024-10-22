-- Query the Snowflake metadata to find columns with "name" in the name

SELECT
    table_schema
    , table_name
    , column_name
    , data_type
FROM
    source.information_schema.columns
WHERE
    table_schema = 'NETSUITE'
AND column_name ILIKE '%name%' 
ORDER BY
    table_name
    , column_name;