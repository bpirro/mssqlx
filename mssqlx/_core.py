""""Data Warehouse database connection utilities

Utilities for interacting with data warehouse database objects.
"""

import numpy as np
import pandas as pd
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.sql import text as sa_text


class SqlServerClientError(Exception):
    pass


# class DatabaseClient():
#     """Base interface class for database connections."""
#     create_table(self, schema_name: str, table_name: str, df: pd.DataFrame, **kwargs) -> None:
#         pass


class SqlServerClient:
    """
    Class that serves as client to a SQL Server database

    Attributes:
    server_name (str): The name of the SQL Server instance.
    database_name (str): The name of the default database.

    Methods:
    dataframe_to_table(table_name, schema_name, df, method): writes a Pandas dataframe to a SQL table
    get_table_dataframe(schema_name, table_name): returns a Pandas dataframe from a SQL table
    get_query_dataframe(query): Returns a dataframe from a SQL query result set.
    truncate_table(schema_name, table_name): Truncates SQL table.
    execute_command(sql_cmd): Executes a SQL command.
    disconnect(): Disposes of the sql alchemy engine connection
    """

    def __init__(self, server_name: str, database_name: str) -> None:
        """
        Initalizes a SqlServerClient object.

        :param server_name: Name of Sql Server instancee
        :param database_name: Name of the database
        """
        # self.df = None
        # Build SQL connection string
        self.connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" \
                            f"SERVER={server_name};" \
                            f"DATABASE={database_name};" \
                            "Trusted_Connection=Yes;"

        self.connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.connection_string})

        self.engine = create_engine(self.connection_url)

    # def __del__(self):
    #     """SqlServerClient destructor"""
    #     self.engine.dispose()

    def _parse_sql_name(self, table_name: str, schema_name: str = 'dbo') -> list():
        """Parses full table name into separate schema and table names"""
        table_name_parts: list = table_name.replace('[', '').replace(']', '').split('.')
        if len(table_name_parts) > 2:
            raise SqlServerClientError('Provided table name is not supported.')
        elif len(table_name_parts) == 2:
            schema_name = table_name_parts[0]
            table_name = table_name_parts[1]
        else:
            table_name = table_name_parts[0]
        return schema_name, table_name

    def dataframe_to_table(self, df: pd.DataFrame, table_name: str, schema_name: str = 'dbo', method: str = 'reload') -> None:
        # def create_sql_table(self, **kwargs) -> None:
        """
        Creates SQL table with dataframe data.
        :param df: Dataframe to be loaded into SQL table.
        :param table_name: SQL table name.
        :param schema_name: Schema name of SQL table.
        :param method: How the data is loaded; reload, append or create
        :return:
        """
        schema_name, table_name = self._parse_sql_name(table_name, schema_name)

        if method == 'reload':
            update_type = 'append'
            self.truncate_table(schema_name=schema_name, table_name=table_name)
        elif method == 'create':
            update_type = 'replace'
        elif method == 'append':
            update_type = 'append'

        df.to_sql(table_name, schema=schema_name, con=self.engine, if_exists=update_type, index=False)

    def get_table_dataframe(self, table_name: str, schema_name: str = 'dbo') -> None:
        """
        Returns a dataframe from a SQL table.

        :param table_name:
        :param schema_name:
        :return:
        """
        schema_name, table_name = self._parse_sql_name(table_name, schema_name)

        # Load SQL data into a dataframe
        sql_cmd = f"""SELECT * FROM [{schema_name}].[{table_name}]"""

        with self.engine.begin() as conn:
            df = pd.read_sql_query(con=conn, sql=sql_cmd)
        df = df.replace({np.nan: None})
        return df

    def get_query_dataframe(self, query) -> pd.DataFrame:
        """
        Returns a dataframe from a SQL query result set.

        :param query: SQL query that returns dataset.
        :return: Pandas DataFrmae
        """
        # Load SQL data into a dataframe
        with self.engine.begin() as conn:
            df = pd.read_sql_query(con=conn, sql=query)
        df = df.replace({np.nan: None})
        return df

    def execute_command(self, sql_cmd) -> None:
        """
        Executes a SQL command.

        :param sql_cmd: SQL EXEC query

        :return: None
        """
        sql_cmd = sa_text(sql_cmd)
        with self.engine.begin() as conn:
            result = conn.execute(sql_cmd)

    def truncate_table(self, table_name: str, schema_name: str = 'dbo') -> None:
        """
        Truncates SQL table.

        :param schema_name: Schema name of table.
        :param table_name:  Database table name.

        :return: None
        """
        schema_name, table_name = self._parse_sql_name(table_name, schema_name)

        with self.engine.begin() as conn:
            conn.execute(sa_text(f'''TRUNCATE TABLE [{schema_name}].[{table_name}]''').execution_options(autocommit=True))

    def execute_sp(self, stored_proc_name: str, schema_name: str = 'dbo') -> None:
        """
        Executes SQL stored procedure.

        :param stored_proc_name: Name of procedure in schema.table or table format
        :param schema_name: Optional name of schema

        :return: None
        """
        schema_name, stored_proc_name = self._parse_sql_name(stored_proc_name, schema_name)

        with self.engine.begin() as conn:
            conn.execute(sa_text(f'''EXECUTE [{schema_name}].[{stored_proc_name}]''').execution_options(autocommit=True))

    def disconnect(self) -> None:
        """Method to close the open database connection."""
        self.engine.dispose()
