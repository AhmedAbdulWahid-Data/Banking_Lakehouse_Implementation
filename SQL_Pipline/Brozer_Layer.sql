-----------------------------------------------------
------------ Data Cleaning - Customers --------------
-----------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_customers_ingestion_cleaned (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dob EXPECT (dob IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_gender EXPECT (gender IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_city EXPECT (city IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_join_date EXPECT (join_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (
    email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_phone_number EXPECT (phone_number IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_channel EXPECT (preferred_channel IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_occupation EXPECT (occupation IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_income_range EXPECT (income_range IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_risk_segment EXPECT (risk_segment IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "This table contains the cleaned data from Landing customers_incremental"
AS SELECT
  customer_id,
  UPPER(name) AS name,
  LOWER(email) AS email,
  UPPER(occupation) AS occupation,
  UPPER(city) AS city,
  UPPER(income_range) AS income_range,
  preferred_channel,
  -- Date Standardization
  COALESCE(
    to_date(trim(join_date), "MM/dd/yyyy"),
    to_date(trim(join_date), "yyyy-MM-dd"),
    to_date(trim(join_date), "M/d/yyyy")
  ) AS join_date,
  COALESCE(
    to_date(trim(dob), "MM/dd/yyyy"),
    to_date(trim(dob), "yyyy-MM-dd"),
    to_date(trim(dob), "M/d/yyyy")
  ) AS dob,
  -- Gender Mapping
  CASE 
    WHEN LOWER(TRIM(gender)) = 'm' THEN 'MALE'
    WHEN LOWER(TRIM(gender)) = 'f' THEN 'FEMALE'
    ELSE 'UNKNOWN' 
  END AS gender,
  -- Status Mapping
  CASE 
    WHEN status IS NULL OR TRIM(status) = '' THEN 'UNKNOWN'
    ELSE UPPER(TRIM(status))
  END AS status,
  -- Risk Segment Mapping
  CASE 
    WHEN UPPER(TRIM(risk_segment)) IN ('HIGH', 'MEDIUM', 'LOW', 'UNKNOWN') THEN UPPER(TRIM(risk_segment))
    ELSE 'UNKNOWN'
  END AS risk_segment,
  -- Phone Number Cleaning (Filtering handled in WHERE clause)
  regexp_replace(phone_number, '[^\\d\\+]', '') AS phone_number
FROM STREAM(landing_customers_incremental)
WHERE regexp_replace(phone_number, '[^\\d\\+]', '') RLIKE '^\\+44\\d{10}$';




-----------------------------------------------------------
------------- Data Cleaning - Transactions ----------------
-----------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze_transactions_ingestion_cleaned (
  CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_txn_date EXPECT (txn_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_txn_amount EXPECT (txn_amount IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_txn_type EXPECT (txn_type IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_type EXPECT (account_type IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_txn_channel EXPECT (txn_channel IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_balance EXPECT (balance IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "This table contains the cleaned data from the landing account transactions incremental"
AS SELECT
  account_id,
  txn_id,
  customer_id,
  txn_amount,
  balance,
  UPPER(account_type) AS account_type,
  UPPER(txn_channel) AS txn_channel,
  -- Date Standardization
  COALESCE(
    to_date(trim(txn_date), "MM/dd/yyyy"),
    to_date(trim(txn_date), "yyyy-MM-dd"),
    to_date(trim(txn_date), "M/d/yyyy")
  ) AS txn_date,
  -- Transaction Type Mapping
  CASE 
    WHEN UPPER(TRIM(txn_type)) IN ('DEBITT', 'DEBIT') THEN 'DEBIT'
    WHEN UPPER(TRIM(txn_type)) IN ('CREDIIT', 'CREDIT') THEN 'CREDIT'
    ELSE txn_type
  END AS txn_type
FROM STREAM(landing_accounts_transactions_incremental);