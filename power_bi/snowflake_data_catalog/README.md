En-US
# Description
This project aims to facilitate the presentation of basic data to start a data governance in a Snowflake enviroment, using views from the platform itself as a source and Power BI as a visualization tool.

[Click here to access the sample report.](https://app.powerbi.com/view?r=eyJrIjoiNzUwYzExMWYtYjkyMC00Nzc4LThkNjYtZWRkZmFhMmY3NTI5IiwidCI6IjkyZGVmYjkyLWQwZWYtNDZkZC04OWQ1LTdlN2JmZTIzMmYwNSJ9)

# Configure Power BI File

To update the .pbix file with your data you'll need a user with read access in the following objects:

- ACCOUNT_USAGE.TABLES
- ACCOUNT_USAGE.COLUMNS
- ACCOUNT_USAGE.COPY_HISTORY
- ACCOUNT_USAGE.ACCESS_HISTORY
- ACCOUNT_USAGE.QUERY_HISTORY
- ACCOUNT_USAGE.OBJECT_DEPENDENCIES

*Ps.: The following documentation explain the steps to grant all the required access ([click here](https://docs.snowflake.com/en/sql-reference/account-usage#enabling-the-snowflake-database-usage-for-other-roles)), be aware that this way you'll grant access to the entire ACCOUNT_USAGE schema, if you only want to grant the minimum necessary access check the file "create_role.sql".*

Now you just need to configure the Power Query parameters, refresh the dataset and that's it!

Power Query Parameters:
- pr_snowflake_acct: Snowflake account in the following format: \<snowflake-account\>.snowflakecomputing.com
- pr_snowflake_db: Database with the ACCOUNT_USAGE views
- pr_snowflake_warehouse: Snoflake Warehouse (Eg.: COMPUTE_WH)

--------
Pt-BR

# Descrição
Esse projeto tem como objetivo facilitar a apresentação de dados básicos para iniciar um fluxo de governança de dados em uma arquitetura com Snowflake, utilizando views da própria plataforma como fonte e com uso do Power BI como ferramenta de visualização.

[Clique aqui para acessar o relatório de exemplo.](https://app.powerbi.com/view?r=eyJrIjoiNzUwYzExMWYtYjkyMC00Nzc4LThkNjYtZWRkZmFhMmY3NTI5IiwidCI6IjkyZGVmYjkyLWQwZWYtNDZkZC04OWQ1LTdlN2JmZTIzMmYwNSJ9)

# Configuração do Power BI

Para atualizar o .pbix com os seus dados é necessário utilizar um usuario que tenha acesso de leitura nos seguintes objetos:

- ACCOUNT_USAGE.TABLES
- ACCOUNT_USAGE.COLUMNS
- ACCOUNT_USAGE.COPY_HISTORY
- ACCOUNT_USAGE.ACCESS_HISTORY
- ACCOUNT_USAGE.QUERY_HISTORY
- ACCOUNT_USAGE.OBJECT_DEPENDENCIES

*Obs.: A documentação a seguir mostrar o passo a passo de como conceder os acessos necessários ([clique aqui](https://docs.snowflake.com/en/sql-reference/account-usage#enabling-the-snowflake-database-usage-for-other-roles)), é importante comentar que que dessa forma você dará acesso ao schema inteiro do ACCOUNT_USAGE, caso queira conceder acesso somente as tabelas mencionadas confira o script "create_role.sql".*

Com o úsuario em mãos agora basta configurar os parâmetros do Power Query e atualizar o dataset!

Parâmetros Power Query:
- pr_snowflake_acct: Conta do Snowflake com o seguinte formato: \<snowflake-account\>.snowflakecomputing.com
- pr_snowflake_db: Banco de dados com as views do ACCOUNT_USAGE
- pr_snowflake_warehouse: Snoflake Warehouse (Eg.: COMPUTE_WH)