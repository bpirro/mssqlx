"""
Smartsheet API wrapper data client.

Provide utility and data handling methods for interacting with Smartsheet via a wrapper on the smartsheet api/sdk.

:author: Brian Pirro
"""


import smartsheet

from pandas import DataFrame
from mssqlx.crud import SqlServerClient


class SmartsheetClient(smartsheet.Smartsheet):
    """Class for running ETL on Smartsheet"""

    def __init__(self, access_token):
        self._access_token = access_token
        self.client = smartsheet.Smartsheet(self._access_token)
        self.client.errors_as_exceptions(True)

        # self.server_name = kwargs.get('server_name', 'localhost')
        # self.database_name = kwargs.get('database_name', 'TEST_DB')

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
        data_dict['RowId'], data_dict['RowNumber'] = list(), list()
        for column_name, value in columns_dict.items():
            data_dict[column_name] = list()

        # populate ss data dictionary
        for row in sheet.rows:
            data_dict['RowId'].append(int(row.id))
            data_dict['RowNumber'].append(int(row.row_number))
            for column_map_name, column_map_id in columns_dict.items():
                field_value = row.get_column(column_map_id).value
                data_dict[column_map_name].append(field_value)

        # convert ss data dictionary to dataframe
        df = DataFrame(data_dict).infer_objects()

        # drop rows that contain all blank values
        user_columns = list(df.columns)[1:]
        df = df.dropna(axis=0, how='all', subset=user_columns)

        return df


if __name__ == '__main__':
    pass
