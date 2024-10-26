""""Sql

Utilities for interacting with data warehouse database objects.
"""

import numpy as np
import pandas as pd
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.sql import text as sa_text

# class DatabaseClient():
#     """Base interface class for database connections."""
#     create_table(self, schema_name: str, table_name: str, df: pd.DataFrame, **kwargs) -> None:
#         pass


class SqlServerClient:
    """Base SQL class"""
    def __init__(self, server_name: str, database_name: str) -> None:
        # Build SQL connection string
        self.df = pd.DataFrame()
        self.connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" \
                            f"SERVER={server_name};" \
                            f"DATABASE={database_name};" \
                            "Trusted_Connection=Yes;"

        self.connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.connection_string})

        self.engine = create_engine(self.connection_url)

    def create_table(self, schema_name: str, table_name: str, df: pd.DataFrame, **kwargs) -> None:
        # def create_sql_table(self, **kwargs) -> None:
        """Creates SQL table with dataframe data.

        :param str schema_name: Schema name of the SQL table being reloaded.
        :param str table_name: Table name of the SQL table being reloaded.
        :param pd.Dataframe df: Dataframe data to be inserted into SQL table.
        :return: None
        """

        df.to_sql(table_name, schema=schema_name, con=self.engine, if_exists='replace', index=False)

    def reload_table(self, schema_name: str, table_name: str, df: pd.DataFrame) -> None:
        """Reloads SQL table with dataframe data.

        :param str schema_name: Schema name of the SQL table being reloaded.
        :param str table_name: Table name of the SQL table being reloaded.
        :param pd.Dataframe df: Dataframe data to be inserted into SQL table.
        :return: None
        """
        with self.engine.begin() as conn:
            conn.execute(
                sa_text(f'''TRUNCATE TABLE [{schema_name}].[{table_name}]''').execution_options(autocommit=True))
        df.to_sql(table_name, schema=schema_name, con=self.engine, if_exists='append', index=False)

    def append_table(self, schema_name: str, table_name: str, df: pd.DataFrame, **kwargs) -> None:
        # def create_sql_table(self, **kwargs) -> None:
        """Appends rows to SQL table from dataframe data.

        :param str schema_name: Schema name of the SQL table being reloaded.
        :param str table_name: Table name of the SQL table being reloaded.
        :param pd.Dataframe df: Dataframe data to be inserted into SQL table.
        :return: None
        """
        df.to_sql(table_name, schema=schema_name, con=self.engine, if_exists='append', index=False)

    def get_table_dataframe(self, schema_name: str, table_name: str):
        """Returns a dataframe from a SQL table."""
        # Load SQL data into a dataframe
        sql_cmd = f"""SELECT * FROM [{schema_name}].[{table_name}]"""
        self.df = pd.read_sql_query(con=self.engine, sql=sql_cmd)
        self.df = self.df.replace({np.nan: None})
        # self.df.fillna('', inplace=True)
        # self.df = self.df.convert_dtypes(dtype_backend='numpy_nullable')
        return self.df

    def get_query_dataframe(self, query):
        """Returns a dataframe from a SQL query result set."""
        # Load SQL data into a dataframe
        self.df = pd.read_sql_query(con=self.engine, sql=query)
        # self.df.fillna(None, inplace=True)
        self.df = self.df.replace({np.nan: None})
        self.df = self.df.astype('str')
        return self.df

    def execute_sql_query(self, sql_cmd):
        """Executes a sql query"""
        sql_cmd = sa_text(sql_cmd)
        with self.engine.begin() as conn:
            result = conn.execute(sql_cmd)

    def truncate_table(self):
        pass

    def backup_database(self):
        pass

    def insert_row(self):
        pass

    def execute_sp(self):
        pass