"""Smartsheet ETL Library

Utility module for exporting or viewing clients data.
"""

import os
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
        data_dict['RowID'] = list()
        for column_name, value in columns_dict.items():
            data_dict[column_name] = list()

        # populate ss data dictionary
        for row in sheet.rows:
            data_dict['RowID'].append(int(row.id))
            for column_map_name, column_map_id in columns_dict.items():
                field_value = row.get_column(column_map_id).value
                data_dict[column_map_name].append(field_value)

        # convert ss data dictionary to dataframe
        df = DataFrame(data_dict).infer_objects()

        # drop rows that contain all blank values
        user_columns = list(df.columns)[1:]
        df = df.dropna(axis=0, how='all', subset=user_columns)

        return df


class SmartsheetIntegration(SmartsheetClient):
    def __init__(
            self,
            access_token: str,
            sheet_id: str,
            schema_name,
            table_name,
            columns_config: str,
            db_client: SqlServerClient
    ) -> None:

        super().__init__(access_token)

        self.sheet_updated = 0
        self.db_client = db_client

        self.sheet_id = sheet_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.id, self.row = None, None
        self.sheet = SmartsheetClient.get_sheet(self, sheet_id)

        self.checkbox_columns: list = list()
        self.currency_columns: list = list()
        for column_name, column_config in columns_config.items():
            if column_config['type'] == 'PRIMARY KEY':
                self.primary_key_ss = column_name
                self.primary_key_sql = column_config.get('sql', column_name)
                # self.primary_key_sql = column_config['sql']
            elif column_config.get('constraint') == 'CHECKBOX':
                self.checkbox_columns.append(column_name)
            elif column_config.get('constraint') == 'CURRENCY':
                self.currency_columns.append(column_name)

        # load the smartsheet and sql table into dataframes
        self.df_ss = self.get_sheet_dataframe(sheet_id)
        self.df_sql = self.db_client.get_table_dataframe(schema_name=schema_name, table_name=table_name)
        # self.df_sql['RowID'].dtype('int64')
        # self.df = self.df.astype({'RowID': 'int64'}).dtypes

        self.columns_config = columns_config

        # build a dictionary that maps SS column names to SQL column names
        self.column_name_map = dict()
        for ss_column, properties in columns_config.items():
            self.column_name_map[ss_column] = properties.get('sql', ss_column)

        # self.currency_columns = currency_columns
        # self.checkbox_columns = checkbox_columns
        # self.columns_dict = {}
        # self.df_ss[self.primary_key] = self.df_ss[self.primary_key].astype(int)
        # self.df_sql[self.primary_key] = self.df_sql[self.primary_key].astype(int)

    def build_smartsheet_row_new(self, dataframe_row):
        """Builds smartsheet row"""
        self.row = smartsheet.models.Row()
        self.row.to_bottom = True

        for column_name, attributes in self.columns_config.items():
            if attributes['type'] in ('PUSH', 'POPULATE'):
                sql_column_name = attributes.get('sql', column_name + '_sql')
                # sql_column_name = column_name + '_sql'
                # if str(dataframe_row[sql_column_name]) and not isinstance(dataframe_row[sql_column_name], type(None)):
                if column_name in self.currency_columns:
                    cell = smartsheet.Smartsheet.models.Cell()
                    cell.strict = False
                    cell.format = ',,,,,,,,,,,13,0,1,2,,'
                    # set decimals to two decimal places
                    value = str(format(float(dataframe_row[sql_column_name]), '.2f'))
                elif column_name in self.checkbox_columns:
                    cell = smartsheet.Smartsheet.models.Cell()
                    cell.strict = False
                    value = str(dataframe_row[sql_column_name])
                else:
                    cell = smartsheet.Smartsheet.models.Cell()
                    cell.strict = True
                    value = str(dataframe_row[sql_column_name])
            elif attributes['type'] == 'PRIMARY KEY':
                cell = smartsheet.Smartsheet.models.Cell()
                value = str(dataframe_row[column_name])
            else:
                continue
            cell.column_id = self.get_column_id(column_name)
            cell.value = value
            self.row.cells.append(cell)
        return self.row

    def merge_smartsheet_inserts(self) -> None:
        # build dataframe of new rows to be inserted into smartsheet
        df_new = pd.merge(self.df_sql, self.df_ss, how='left', left_on=self.primary_key_sql, right_on=self.primary_key_ss,
                          suffixes=('_sql', '_ss')).infer_objects()
        # df_new = df_new[pd.isnull(df_new[self.primary_key_ss])]

        new_rows_ss = list()
        for index, row in df_new.iterrows():
            new_row_ss = self.build_smartsheet_row_new(dataframe_row=row)
            # new_row_ss = self.build_smartsheet_row_new(sheet=self.sheet_id, dataframe_row=row,
            #                                            column_dict=self.columns_dict)
            new_rows_ss.append(new_row_ss)
        if len(new_rows_ss) > 0:
            try:
                # smartsheet.Smartsheet.models.Sheet.add_rows(self.sheet_id, new_rows_ss)
                self.sheet.add_rows(new_rows_ss)
            except Exception as e:
                raise e

    def merge_smartsheet_updates(self):
        """Update Smartsheet fields values based upon corresponding SQL field value"""
        # join sql and ss dataframes on primary key field
        df_match = pd.merge(self.df_sql, self.df_ss, how='inner', left_on=self.primary_key_sql,
                            right_on=self.primary_key_ss, suffixes=('_sql', '_ss'))

        columns_pushed = {column_name: attributes for column_name, attributes in self.columns_config.items()
                          if attributes['type'] == 'PUSH'}
        # columns_pushed = [key for key in self.columns_dict.keys() if self.columns_dict[key] == 'PUSH']

        # loop through each row in the matched dataframe
        update_rows = list()
        for idx, row in df_match.iterrows():
            ss_cells = list()
            # loop through each column in the row
            for column_name, attributes in columns_pushed.items():
                sql_column_name = attributes.get('sql', column_name)
                if column_name == sql_column_name:
                    ss_value = row[column_name + '_ss']
                    sql_value = row[sql_column_name + '_sql']
                else:
                    ss_value = row[column_name]
                    sql_value = row[sql_column_name]
                # create a new, updated cell if the sql value does not match ss value
                if ss_value == sql_value:
                    continue
                else:
                    cell = self.client.models.Cell()
                    cell.column_id = self.get_column_id(column_name=column_name)

                    # format currency data
                    if column_name in self.currency_columns:
                        cell.strict = False
                        cell.format = ',,,,,,,,,,,13,0,1,2,,'
                        cell_value = str(format(float(sql_value), '.2f'))
                    else:
                        cell.strict = True
                        cell_value = sql_value

                    cell.value = cell_value
                    cell.display_value = cell_value

                    # append update cells to list
                    ss_cells.append(cell)

            # Build a new row if there are any updated cells
            if len(ss_cells) > 0:
                ss_row = self.client.models.Row()
                ss_row.id = int(row['RowID'])
                for cell in ss_cells:
                    ss_row.cells.append(cell)
                update_rows.append(ss_row)

        if len(update_rows) > 0:
            try:
                self.client.Sheets.update_rows(self.sheet_id, update_rows)
                smartsheet_updated = True
            except Exception as e:
                raise e

    def merge_smartsheet_deletes(self):
        pass

    def merge_sql_inserts(self):
        pass

    def merge_sql_updates(self):
        """Update Smartsheet fields values based upon corresponding SQL field value"""
        # join sql and ss dataframes on primary key field
        df_match = pd.merge(self.df_sql, self.df_ss, how='inner', left_on=self.primary_key_sql,
                            right_on=self.primary_key_ss, suffixes=('_sql', '_ss'))

        columns_pulled = {column_name: attributes for column_name, attributes in self.columns_config.items()
                          if attributes['type'] == 'PULL'}
        # columns_pulled = [key for key, value in self.columns_dict.items() if value['type'] == 'PULL']
        # columns_pulled = [key for key in self.columns_dict.keys() if self.columns_dict[key] == 'PULL']

        # loop through each row in the matched dataframe
        update_rows_sql = dict()
        for idx, row in df_match.iterrows():
            # loop through each column in the row
            update_columns_sql = dict()
            for column_name, attributes in columns_pulled.items():
                sql_column_name = attributes.get('sql', column_name)
                if column_name == sql_column_name:
                    ss_value = row[column_name + '_ss']
                    sql_value = row[column_name + '_sql']
                else:
                    ss_value = row[column_name]
                    sql_value = row[sql_column_name]
                # create a new, updated cell if the sql value does not match ss value
                if ss_value == sql_value:
                    continue
                # elif column in self.currency_columns and not is_number(ss_value):
                #     continue
                else:
                    pass
                    if ss_value is not None:
                        ss_value = ss_value.replace("'", "''")
                    else:
                        ss_value = None
                    update_columns_sql[column_name] = ss_value
            if not update_columns_sql:
                continue
            else:
                update_rows_sql[row[self.primary_key_sql]] = update_columns_sql

        if update_rows_sql:
            sql_cmd, sql_cmds = '', ''
            sql_update_count, sql_update_max_count = 0, 100
            # loop through all entries with pending updates
            for key, value in update_rows_sql.items():
                sql_cmd = f"""UPDATE [{self.schema_name}].[{self.table_name}]\nSET """
                primary_key_value = key
                sql_update_clause = ''
                for column_name, column_value in value.items():
                    if column_value == '' or column_value is None:
                        sql_update_clause += f"[{column_name}] = NULL, "
                    else:
                        sql_update_clause += f"[{column_name}] = '{column_value}', "

                sql_cmd += sql_update_clause[:-2]
                # sql_cmd += f"\nWHERE CONVERT(VARCHAR(32), [{primary_key}], 2) = '{primary_key_value}'\n"
                sql_cmd += f"\nWHERE [{self.primary_key_sql}] = '{primary_key_value}'\n"

                # self.db_client.execute_sql_query(sql_cmd)
                # print(sql_cmd)
                # exit()

                sql_cmds += sql_cmd
                sql_update_count += 1

                # perform the sql updates in batches
                if sql_update_count >= sql_update_max_count:
                    # print(sql_cmds)
                    self.db_client.execute_sql_query(sql_cmds)   # todo: add try block
                    sql_cmds = ''
                    sql_update_count = 0

            # perform any remaining updates
            if len(sql_cmds) > 0:
                self.db_client.execute_sql_query(sql_cmds)   # todo: add try block

    def merge_sql_deletes(self):
        pass

    def get_column_id(self, column_name):
        """Returns a column id for a given column name"""
        column_map = dict()
        for column in self.sheet.columns:
            column_map[column.title] = column.id
        self.id = column_map[column_name]
        return self.id
