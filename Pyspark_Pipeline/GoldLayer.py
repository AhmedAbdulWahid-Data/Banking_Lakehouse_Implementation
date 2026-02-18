import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="gold_cust_acc_trns_mv",
  comment="Customer Account Transactions Materialized View"
)
def gold_cust_acc_trns_mv():
    customers = dlt.read("silver_customers_transformed")
    tx = dlt.read("silver_account_transactions_transformed")

    # Rename transformation_date in tx to avoid duplication
    tx = tx.withColumnRenamed(
        "transformation_date",
        "tx_transformation_date"
    )

    joined = customers.join(
        tx,
        on="customer_id",
        how="inner"
    )
    return joined


########### AGGREGATIONS ###############

@dlt.table(
  name="gold_cust_acc_trns_agg",
  comment="Customer Account Transactions Aggregations"
)
def gold_cust_acc_trns_agg():
    df = dlt.read("gold_cust_acc_trns_mv")
    
    return (
        df.groupBy(
            "customer_id", "name", "gender", "city", "status",
            "income_range", "risk_segment","customer_age", "tenure_days"
        ).agg(
            countDistinct("account_id").alias("total_accounts"),
            count("*").alias("total_transactions"),
            round(
                sum(
                    when(col("txn_type") == "CREDIT", col("txn_amount"))
                    .otherwise(lit(0.0))
                ), 2
            ).alias("total_credits"),
            round(
                sum(
                    when(col("txn_type") == "DEBIT", col("txn_amount"))
                    .otherwise(lit(0.0))
                ), 2
            ).alias("total_debits"),
            round(avg(col("txn_amount")), 2).alias("avg_txn_amount"),
            countDistinct(col("txn_channel")).alias("total_txn_channels"),
            min(col("txn_date")).alias("min_txn_date"),
            max(col("txn_date")).alias("max_txn_date")
        )
    )