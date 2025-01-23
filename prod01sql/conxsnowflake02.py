import os
import great_expectations as gx

USER_NAME = 'SystaxSnow24'
ACCOUNT_NAME = 'djdyjny-ZK69750'
PASSWORD = "Dkjj$@8$g@hgsgj!!"
   
ROLE_NAME = "ACCOUNTADMIN"
DATABASE_NAME = "DB_TABELAO"
SCHEMA_NAME = "DBO"
WAREHOUSE_NAME = "COMPUTE_WH"

snowflake_connection_str = (
    f"snowflake://{USER_NAME}:{PASSWORD}@{ACCOUNT_NAME}/{DATABASE_NAME}/{SCHEMA_NAME}?"
    f"warehouse={WAREHOUSE_NAME}&role={ROLE_NAME}&authenticator=externalbrowser"
)

context = gx.get_context()

datasource = context.sources.add_snowflake(
    name="snowflake_data_source",
    connection_string=snowflake_connection_str
)