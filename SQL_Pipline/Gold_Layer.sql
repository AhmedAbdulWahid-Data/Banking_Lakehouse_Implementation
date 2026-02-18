CREATE OR REFRESH MATERIALIZED VIEW gold_cust_acc_trns_mv
COMMENT "Customer Account Transactions Materialized View"
AS 
SELECT 
  c.*,
  t.* EXCEPT (customer_id, transformation_date),
  t.transformation_date AS tx_transformation_date
FROM LIVE.silver_customers_transformed c
INNER JOIN LIVE.silver_account_transactions_transformed t
  ON c.customer_id = t.customer_id;

CREATE OR REFRESH MATERIALIZED VIEW gold_cust_acc_trns_agg
COMMENT "Customer Account Transactions Aggregations"
AS
SELECT
  customer_id,
  name,
  gender,
  city,
  status,
  income_range,
  risk_segment,
  customer_age,
  tenure_days,
  COUNT(DISTINCT account_id) AS total_accounts,
  COUNT(*) AS total_transactions,
  ROUND(SUM(CASE WHEN txn_type = 'CREDIT' THEN txn_amount ELSE 0.0 END), 2) AS total_credits,
  ROUND(SUM(CASE WHEN txn_type = 'DEBIT' THEN txn_amount ELSE 0.0 END), 2) AS total_debits,
  ROUND(AVG(txn_amount), 2) AS avg_txn_amount,
  COUNT(DISTINCT txn_channel) AS total_txn_channels,
  MIN(txn_date) AS min_txn_date,
  MAX(txn_date) AS max_txn_date
FROM gold_cust_acc_trns_mv
GROUP BY 
  customer_id, name, gender, city, status, 
  income_range, risk_segment, customer_age, tenure_days;