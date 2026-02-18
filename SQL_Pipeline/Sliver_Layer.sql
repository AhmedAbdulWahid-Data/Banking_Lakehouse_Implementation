------------------------------------------------------------------------
--------------- Data Transformation - Customer Table--------------------
------------------------------------------------------------------------


-- Step 1: Transformation Table (Streaming)
CREATE OR REFRESH STREAMING TABLE silver_customers_transformed
COMMENT "Transformed customers table with age and tenure calculations"
AS SELECT 
  *,
  -- Calculate Age
  CASE WHEN dob IS NOT NULL THEN FLOOR(months_between(current_date(), dob) / 12) 
       ELSE NULL END AS customer_age,
  -- Calculate Tenure in Days
  CASE WHEN join_date IS NOT NULL THEN datediff(current_date(), join_date) 
       ELSE NULL END AS tenure_days,
  -- Flag out of range dates
  (dob < '1900-01-01' OR dob > current_date()) AS dob_out_of_range_flag,
  current_timestamp() AS transformation_date
FROM STREAM(bronze_customers_ingestion_cleaned);

-- Step 2: Target SCD Type 1 Table
CREATE OR REFRESH STREAMING TABLE silver_customers_transformed_scd1;

CREATE FLOW silver_customers_transformed_scd1_flow AS
AUTO CDC INTO silver_customers_transformed_scd1
FROM STREAM(silver_customers_transformed)
KEYS (customer_id)
SEQUENCE BY transformation_date
COLUMNS * EXCEPT (transformation_date)
STORED AS SCD TYPE 1;

-- Step 3: View
CREATE TEMPORARY VIEW silver_customers_transformed_view
COMMENT "View of silver_customers_transformed table"
AS SELECT * FROM STREAM(silver_customers_transformed);




------------------------------------------------------------------------
--------------- Data Transformation - Transactions Table----------------
------------------------------------------------------------------------


-- Step 1: Transformation Table (Streaming)
CREATE OR REFRESH STREAMING TABLE silver_account_transactions_transformed
COMMENT "Transformed account_transactions table with channel and date parts"
AS SELECT
  *,
  -- Channel Classification
  CASE WHEN txn_channel IN ('ATM', 'BRANCH') THEN 'PHYSICAL' 
       ELSE 'DIGITAL' END AS channel_type,
  -- Date Parts
  YEAR(txn_date) AS txn_year,
  MONTH(txn_date) AS txn_month,
  DAYOFMONTH(txn_date) AS txn_day,
  -- Transaction Direction
  CASE WHEN txn_type = 'DEBIT' THEN 'OUT' 
       ELSE 'IN' END AS txn_direction,
  current_timestamp() AS transformation_date
FROM STREAM(bronze_transactions_ingestion_cleaned);

-- Step 2: Target SCD Type 2 Table
CREATE OR REFRESH STREAMING TABLE silver_account_transactions_transformed_scd2;

CREATE FLOW silver_account_transactions_transformed_scd2_flow AS
AUTO CDC INTO silver_account_transactions_transformed_scd2
FROM STREAM(silver_account_transactions_transformed)
KEYS (txn_id)
SEQUENCE BY transformation_date
COLUMNS * EXCEPT (transformation_date)
STORED AS SCD TYPE 2;

-- Step 3: View
CREATE TEMPORARY VIEW silver_account_transactions_transformed_view
COMMENT "View of silver_account_transactions_transformed table"
AS SELECT * FROM STREAM(silver_account_transactions_transformed);