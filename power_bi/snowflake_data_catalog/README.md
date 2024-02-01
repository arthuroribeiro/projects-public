# Configure Power BI File

To update the .pbix file with your data you'll need a user with read access in the following objects:

- ACCOUNT_USAGE.TABLES
- ACCOUNT_USAGE.COLUMNS
- ACCOUNT_USAGE.COPY_HISTORY
- ACCOUNT_USAGE.ACCESS_HISTORY
- ACCOUNT_USAGE.QUERY_HISTORY
- ACCOUNT_USAGE.OBJECT_DEPENDENCIES

*Ps.: The steps to grant the required access are in this documentation ([click here](https://docs.snowflake.com/en/sql-reference/account-usage#enabling-the-snowflake-database-usage-for-other-roles)), but this way you'll grant access to the entire ACCOUNT_USAGE schema, if you only want to grant the minimium required access check the file "create_role.sql".*

The next step is to configure the Power Query parameters, refresh the dataset and that's it!

Power Query Parameters:
- pr_snowflake_acct: Snowflake account in the following format: \<snowflake-account\>.snowflakecomputing.com
- pr_snowflake_db: Database with the ACCOUNT_USAGE views
- pr_snowflake_warehouse: Snoflake Warehouse (Eg.: COMPUTE_WH)
