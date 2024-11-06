"""Test Unit for _core module."""

# todo: setup formal unit tests with asserts

import mssqlx._core

# print(mssqlx._core.__dict__)

# create SqlServerClient
db_client = mssqlx.SqlServerClient(server_name='localhost', database_name='mssqlx')

df = mssqlx.pd.DataFrame({'Id': [1, 2, 3], 'Value': ['aaa', 'bbb', None]})

db_client.dataframe_to_table(schema_name='dbo', table_name='test', df=df, method='create')

df = db_client.get_table_dataframe(table_name='[dbo].[test]')

# df = db_client.get_table_dataframe(schema_name='dbo', table_name='test')
df_out = db_client.get_query_dataframe('SELECT TOP 2 * FROM dbo.test')

db_client.execute_command(sql_cmd='EXEC sp_who2')

db_client.disconnect()

pass