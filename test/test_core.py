import mssqlx

# create SqlServerClient
db_client = mssqlx.SqlServerClient(server_name='localhost', database_name='mssqlx')

df = mssqlx.pd.DataFrame({'Id': [1, 2, 3], 'Value': ['aaa', 'bbb', 'ccc']})

db_client.dataframe_to_table(schema_name='dbo', table_name='test', df=df)
db_client.dataframe_to_table(table_name='test1', df=df, method='create')
db_client.dataframe_to_table(table_name='[test].test3', df=df, method='create')
db_client.dataframe_to_table(table_name='[test].[test4]', df=df, method='create')


df = db_client.get_table_dataframe(schema_name='dbo', table_name='test')

pass