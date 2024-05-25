"""Smartsheet ETL Library

Utility module for exporting or viewing clients data.

2023-02-26 - Created by Brian Pirro
"""

import os
import smartsheet
from pandas import DataFrame
from dwlib.dwlib.db import SqlServerClient


class SmartsheetClient(smartsheet.Smartsheet):
    """Class for running ETL on Smartsheet"""
    def __init__(self, access_token, DbClient=SqlServerClient, **kwargs):
        self._access_token = access_token
        self.client = smartsheet.Smartsheet(self._access_token)
        self.client.errors_as_exceptions(True)

        self.server_name = kwargs.get('server_name', 'localhost')
        self.database_name = kwargs.get('database_name', 'TEST_DB')

        # super().__init__(server_name=self.server_name, database_name=self.database_name)
        self.dbclient = DbClient(self.server_name, self.database_name)

    def print_sheets(self) -> None:
        """Displays summary of sheets accessible to client.

        :param: None
        :return: None
        """
        response = self.client.Sheets.list_sheets(include_all=True)
        sheets = response.data
        print('Sheet_ID', 'Sheet_Name', 'Hyperlink', sep=', ')
        for sheet in sheets:
            print(sheet.id, sheet.name, sheet.permalink, sep=', ')

    def print_sheets_details(self) -> None:
        """Displays details of sheets accessible to client.

        :param: None
        :return: None
        """
        response = self.client.Sheets.list_sheets(include_all=True)
        sheets = response.data
        print('Sheet_ID', 'Sheet_Name', 'Access_Level', 'Created_At', 'Modified_At', 'Hyperlink', sep=', ')
        for sheet in sheets:
            print(sheet.id, sheet.name, sheet.rows, sheet.access_level, sheet.created_at, sheet.modified_at,
                  sheet.permalink, sep=', ')

    def print_sheet_columns(self, sheet_id: str) -> None:
        """Displays column names for a given sheet id.

        :param str sheet_id: The id of the sheet.
        :return: None
        """
        sheet = self.client.Sheets.get_sheet(sheet_id)
        for column in sheet.columns:
            print(column.title)

    def print_sheet_column_details(self, sheet_id: str) -> None:
        """Displays column index, id, and name for a given sheet id.

        :param str sheet_id: The id of the sheet.
        :return: None
        """
        sheet = self.client.Sheets.get_sheet(sheet_id)
        for column in sheet.columns:
            print(column.index, column.id, column.title, sep=', ')

    def get_sheet(self, sheet_id: str):
        """Returns a Smartsheet sheet object for a given sheet it."""
        sheet = self.client.Sheets.get_sheet(sheet_id)
        return sheet

    def get_sheet_excel(self, sheet_id: str, download_directory: str) -> None:
        """Downloads an Excel export from a Smartsheet.

        :param str sheet_id: The id of the sheet
        :param str download_directory: The download folder location (ie r'C:/subdir/targetdir')
        :return: None
        """
        self.client.Sheets.get_sheet_as_excel(sheet_id, download_directory)

    def get_sheet_dataframe(self, sheet_id: str) -> DataFrame:
        """Returns a dataframe for a given clients."""
        sheet = self.client.Sheets.get_sheet(sheet_id)

        # build column name to id map dictionary
        columns_dict = dict()
        for column in sheet.columns:
            columns_dict[column.title] = column.id

        # create dictionary of lists to hold ss data
        data_dict = dict()
        data_dict['RowID'] = list()
        for column_name, value in columns_dict.items():
            data_dict[column_name] = list()

        # poplute ss data dictionary
        for row in sheet.rows:
            data_dict['RowID'].append(int(row.id))
            for column_map_name, column_map_id in columns_dict.items():
                field_value = row.get_column(column_map_id).value
                data_dict[column_map_name].append(field_value)

        # convert ss data dictionary to dataframe
        df = DataFrame(data_dict).infer_objects()

        return df

    def create_table(self, sheet_id: str, **kwargs: dict) -> None:
        schema_name = kwargs.get('schema_name', 'dbo')
        table_name = kwargs.get('table_name')
        df = self.get_sheet_dataframe(sheet_id)
        self.dbclient.create_table(schema_name=schema_name, table_name=table_name, df=df)

    def reload_table(self, sheet_id: str, **kwargs: dict) -> None:
        schema_name = kwargs.get('schema_name', 'dbo')
        table_name = kwargs.get('table_name')
        df = self.get_sheet_dataframe(sheet_id)
        
        self.dbclient.reload_table(schema_name=schema_name, table_name=table_name, df=df)


if __name__ == '__main__':
    _ACCESS_TOKEN = os.environ['SS_TOKEN_BRIAN']
    cli = SmartsheetClient(access_token=_ACCESS_TOKEN,
                           DbClient=SqlServerClient,
                           server_name='SAC-DATAANLTC',
                           database_name='BI_ProfServices')
    # cli.print_sheets()
    # cli.print_sheet_columns('1158333194364804')
    # cli.get_sheet_excel('1158333194364804', r'c:\temp')
    cli.create_table(sheet_id='1158333194364804', schema_name='dbo', table_name='CertificationJourneyCentralPharmacySmartsheet2')
    # test_client.download_excel_export('1533182009993092', r'C:\temp')
    # cli.create_table('2551065530552196')
    # cli.reload_table('2551065530552196')


