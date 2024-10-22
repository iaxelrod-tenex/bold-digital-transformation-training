WITH months AS (
    SELECT DISTINCT
        date_month AS month
    FROM
        {{ ref ( 'dim_date' ) }}
)

, contracts AS (
    SELECT
        *
        , DATE_TRUNC ( 'MONTH' , contractstartdate ) AS contractstartmonth
        , DATE_TRUNC ( 'MONTH' , contractenddate ) AS contractendmonth
    FROM
        {{ source ( 'netsuite' , 'contract' ) }}
)

, customers AS (
    SELECT
        *
    FROM
        {{ source ( 'netsuite' , 'customer' ) }}
)

SELECT
    m.month
    , cust.customerid AS customer_id
    , cust.accountnumber AS customer_account_number
    , cust.customername AS customer_name
    , cust.customertype AS customer_type
    , cont.contractid AS contract_id
    , cont.contractnumber AS contract_number
    , cont.contractstatus AS contract_status
    , cont.contracttype AS contract_type
    , cont.contractquantity AS contract_quantity
    , cont.contractunitprice AS contract_unit_price
    , cont.contractmonthlyamount AS contract_monthly_amount
    , cont.contractstartdate AS contract_start_date
    , cont.contractenddate AS contract_end_date
    , cont.contractendreason AS contract_end_reason
    , CASE
        WHEN m.month = cont.contractstartmonth THEN 'GREEN'
        WHEN m.month = cont.contractendmonth THEN 'RED'
        ELSE NULL
    END AS churn_color
    , CASE
        WHEN m.month = cont.contractstartmonth THEN 'NEW'
        WHEN m.month = cont.contractendmonth THEN 'CHURNED'
        ELSE 'ACTIVE'
    END AS churn_label
    , 1 AS active_customers
    , CASE
        WHEN m.month = cont.contractstartmonth THEN 1
        ELSE 0
    END AS new_customers
    , CASE
        WHEN m.month = cont.contractendmonth THEN 1
        ELSE 0
    END AS churned_customers
    , CASE
        WHEN m.month = cont.contractstartmonth THEN 1
        WHEN m.month = cont.contractendmonth THEN -1
        ELSE 0
    END AS net_customers
    , cont.contractmonthlyamount AS active_mrr
    , CASE
        WHEN m.month = cont.contractstartmonth THEN cont.contractmonthlyamount
        ELSE 0
    END AS new_mrr
    , CASE
        WHEN m.month = cont.contractendmonth THEN cont.contractmonthlyamount
        ELSE 0
    END AS churned_mrr
    , CASE
        WHEN m.month = cont.contractstartmonth THEN cont.contractmonthlyamount
        WHEN m.month = cont.contractendmonth THEN -1 * cont.contractmonthlyamount
        ELSE 0
    END AS net_mrr
    , CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS last_refreshed
FROM
    months m
INNER JOIN contracts cont ON m.month BETWEEN cont.contractstartmonth AND COALESCE ( cont.contractendmonth , '2099-12-31' )
INNER JOIN customers cust ON cont.customerid = cust.customerid