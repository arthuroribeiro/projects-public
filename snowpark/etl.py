from loguru import logger
from snowflake.snowpark import Session
import snowflake.snowpark.functions as f
import snowflake.connector
import os

# Configurando conexçao com o Snowflake
# snowflake.connector: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
snow_user = os.getenv("SNOW_USER")  # Usuario do Snowflake
snow_pass = os.getenv("SNOW_PASS")  # Senha do Snowflake
snow_acct = os.getenv("SNOW_ACCT")  # Conta do snowflake, localizada na URL de login. Exemplo: https://<snow_acct>.snowflakecomputing.com/
snow_contexto = snowflake.connector.connect(
    user=snow_user,
    password=snow_pass,
    account=snow_acct,
)
try:
    sql_db = "CREATE DATABASE IF NOT EXISTS ANALYTICS;"
    sql_schema = "CREATE SCHEMA IF NOT EXISTS ANALYTICS.SALES;"
    sql_table = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.SALES.SALES AS
    SELECT *
    FROM (
        VALUES
        (2023, 1, 1, 0.5, 12),
        (2023, 1, 1, 3.2, 28),
        (2023, 1, 12, 8.5, 2),
        (2023, 1, 25, 6.2, 30),
        (2023, 1, 8, 1.9, 19)
    ) AS TEMP (ANO_VENDA, MES_VENDA, DIA_VENDA, PRECO, QUANTIDADE);
    """
    result_db = snow_contexto.cursor().execute(sql_db).fetchall()
    logger.info(f"Database: {result_db}")
    result_schema = snow_contexto.cursor().execute(sql_schema).fetchall()
    logger.info(f"Schema: {result_schema}")
    result_table = snow_contexto.cursor().execute(sql_table).fetchall()
    logger.info(f"Table: {result_table}")
finally:
    snow_contexto.close()

# ETL
connection_parameters = {
    "account": snow_acct,
    "user": snow_user,
    "password": snow_pass,
    "warehouse": "COMPUTE_WH",
}
session = Session.builder.configs(connection_parameters).create()
df = session.table('"ANALYTICS"."SALES"."SALES"')
df = df.withColumn(
    "DATA_VENDA",
    f.concat_ws(
        f.lit("-"),
        f.col("ANO_VENDA"),
        f.col("MES_VENDA"),
        f.col("DIA_VENDA"),
    ).cast("date"),
)
df = df.withColumn("RECEITA", f.col("PRECO") * f.col("QUANTIDADE"))
df = df.select("DATA_VENDA", "QUANTIDADE", "PRECO", "RECEITA")
df.write.mode("overwrite").save_as_table('"ANALYTICS"."SALES"."SALES_ETL"')
logger.info("ETL finalizado")