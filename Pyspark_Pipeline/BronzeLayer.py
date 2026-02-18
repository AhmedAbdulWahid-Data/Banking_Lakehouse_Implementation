import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

################## Data Cleaning - Customers ################

@dlt.table(
    name="bronze_customers_ingestion_cleaned",
    comment="This table contains the cleaned data from Landing customers_incremental"
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect_or_drop("valid_gender", "gender IS NOT NULL")
@dlt.expect_or_drop("valid_city", "city IS NOT NULL")
@dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IS NOT NULL")
@dlt.expect_or_drop(
    "valid_email",
    "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
)
@dlt.expect_or_drop("valid_phone_number", "phone_number IS NOT NULL")
@dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
@dlt.expect_or_drop("valid_occupation", "occupation IS NOT NULL")
@dlt.expect_or_drop("valid_income_range", "income_range IS NOT NULL")
@dlt.expect_or_drop("valid_risk_segment", "risk_segment IS NOT NULL")
def bronze_customers_ingestion_cleaned():
    df = dlt.read_stream(
        "dlt_bank_project_catalog.dlt_bank_project_schema.landing_customers_incremental"
    )

        
    df = df.withColumn("name", upper(col("name")))\
           .withColumn("email", lower(col("email")))\
           .withColumn("occupation", upper(col("occupation")))\
           .withColumn("city", upper(col("city")))\
           .withColumn("risk_segment", upper(col("risk_segment")))\
           .withColumn("income_range", upper(col("income_range"))) 



    df = df.withColumn(
        "join_date",
        coalesce(
            to_date(trim(col("join_date")), "MM/dd/yyyy"),
            to_date(trim(col("join_date")), "yyyy-MM-dd"),
            to_date(trim(col("join_date")), "M/d/yyyy")
     )
    )


    df = df.withColumn(
        "dob",
        coalesce(
            to_date(trim(col("dob")), "MM/dd/yyyy"),
            to_date(trim(col("dob")), "yyyy-MM-dd"),
            to_date(trim(col("dob")), "M/d/yyyy")
     )
    )


    df = df.withColumn(
            "gender",
            when(lower(trim(col("gender"))) == "m", "MALE")
            .when(lower(trim(col("gender"))) == "f", "FEMALE")
            .otherwise("UNKNOWN")
        )
    
    df = df.withColumn(
            "status",
            when(
                col("status").isNull() | (trim(col("status")) == ""),
                lit("UNKNOWN")
            ).otherwise(upper(trim(col("status"))))
        )
    

    # Keep only digits and plus sign
    df = df.withColumn("phone_number", regexp_replace(col("phone_number"), r"[^\d\+]", ""))

    # Filter numbers starting with +44 and 12 digits after +
    df = df.filter(col("phone_number").rlike(r"^\+44\d{10}$"))


        
    df = df.withColumn(
        "risk_segment",
        when(upper(trim(col("risk_segment"))).isin("HIGH", "MEDIUM", "LOW", "UNKNOWN"),
            upper(trim(col("risk_segment"))))
        .otherwise("UNKNOWN")
    )

    return df


############# Data Cleaning - Transactions ################

@dlt.table(
    name="bronze_transactions_ingestion_cleaned",
    comment="This table contains the cleaned data from the landing account transactions incremental"
)


@dlt.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_fail("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")
@dlt.expect_or_drop("valid_account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")

def bronze_transactions_ingestion_cleaned():
    df = dlt.read_stream(
        "dlt_bank_project_catalog.dlt_bank_project_schema.landing_accounts_transactions_incremental"
    )

    df = df.withColumn("account_type", upper(col("account_type")))\
           .withColumn("txn_channel", upper(col("txn_channel")))\
           .withColumn("customer_id", col("customer_id"))
                       
    df = df.withColumn(
        "txn_date",
        coalesce(
            to_date(trim(col("txn_date")), "MM/dd/yyyy"),
            to_date(trim(col("txn_date")), "yyyy-MM-dd"),
            to_date(trim(col("txn_date")), "M/d/yyyy")
     )
    )   

    df = df.withColumn(
        "txn_type",
        when(upper(trim(col("txn_type"))) == "DEBITT", "DEBIT") \
        .when(upper(trim(col("txn_type"))) == "DEBIT", "DEBIT") \
        .when(upper(trim(col("txn_type"))) == "CREDIIT", "CREDIT") \
        .when(upper(trim(col("txn_type"))) == "CREDIT", "CREDIT") \
        .otherwise(col("txn_type"))  
    )
                      

    return df