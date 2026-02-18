------------------ ST for costomers -----------------

create streaming live table landing_customers_incremental
comment 'Incremental landing table for customers'
as
select * from cloud_files('/Volumes/dlt_bank_sql_catalog/dlt_bank_sql_schema/dlt_bank_sql_volume/customers/',"csv",
map(
  "header","true",
  "cloudFiles.inferColumnTypes","true"
));


------------------- ST for Accounts -----------------

create streaming live table landing_accounts_transactions_incremental
comment 'Incremental landing table for accounts'
as
select * from cloud_files('/Volumes/dlt_bank_sql_catalog/dlt_bank_sql_schema/dlt_bank_sql_volume/accounts/',"csv",
map(
  "header","true",
  "cloudFiles.inferColumnTypes","true"
));

