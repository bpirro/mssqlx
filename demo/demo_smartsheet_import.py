import os
from mssqlx.crud import SqlServerClient
from mssqlx.clients.smartsheet_io import SmartsheetClient

# smartsheet
SS_TOKEN = os.getenv('SS_TOKEN')
SS_SHEET_ID = os.getenv('SS_SHEET_ID')

# sql
SERVER = os.getenv('SQL_SERVER_NAME')
DATABASE = os.getenv('SQL_DATABASE_NAME')
SCHEMA = os.getenv('SQL_SCHEMA_NAME')
TABLE = os.getenv('SQL_TABLE_NAME')

sql_client = SqlServerClient(server_name=SERVER, database_name=DATABASE)
ss_client = SmartsheetClient(access_token=SS_TOKEN)

# download smartsheet into dataframe
df = ss_client.get_sheet_dataframe(sheet_id=SS_SHEET_ID)

# import dataframe into SQL table
sql_client.create_table(schema_name=SCHEMA, table_name=TABLE, df=df)
