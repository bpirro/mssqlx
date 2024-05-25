"""SQL Smartsheet Integration

Integrates a SQL table with a Smartsheet. New entries in the SQL table are written to the Smartsheet.  Some column updates are pushed from SQL to Smartsheet, while other columns
are pulled from the Smartsheet into the SQL table.

Notes:
    There must be one column used a primary key in both the SQL table and Smartsheet.
    Multi-column primary keys are currently not supported.

:param Columns_Config - Dictionary of dictionaries with column config data.  Must be configured for


# _COLUMNS_DICT value definitions:
# PRIMARY KEY - field that serves as unique identifier and link between sql table and smartsheet
# PUSH - column fields are compared between smartsheet and sql -> changes are replicated from sql to smartsheet.
# PULL - column fields are compared between smartsheet and sql -> changes are replicated from smartsheet to sql.
# POPULATE - column fields are NOT compared between smartsheet and sql -> field is populated during initial smartsheet
# insert
# IGNORE - column fields are NOT compared between smartsheet and sql and changes are NOT replicated in either direction.

2023-04-18 - Created by Brian Pirro
2024-01-05 - Changes to support new RegionalSalesContact column
"""

import logging
import os
import pandas as pd
import smartsheet

from dwlib.dwclients.smartsheet_dw import SmartsheetClient
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.sql import text as sa_text
from sys import argv


class SmartsheetIntegration(SmartsheetClient):
    def __init__(self, access_token, sheet_id, schema_name, table_name, columns_config, **kwargs):
        super().__init__(access_token, **kwargs)

        self.sheet_updated = 0

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
                self.primary_key_sql = column_config['sql']
            elif column_config.get('constraint') == 'CHECKBOX':
                self.checkbox_columns.append(column_name)
            elif column_config['constraint'] == 'CURRENCY':
                self.currency_columns.append(column_name)

        # load the smartsheet and sql table into dataframes
        self.df_ss = self.get_sheet_dataframe(sheet_id)
        self.df_sql = self.dbclient.get_table_dataframe(schema_name=schema_name, table_name=table_name)
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

        for column_name, attributes in columns_config.items():
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
        df_new = df_new[pd.isnull(df_new[self.primary_key_ss])]

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

                # self.dbclient.execute_sql_query(sql_cmd)
                # print(sql_cmd)
                # exit()

                sql_cmds += sql_cmd
                sql_update_count += 1

                # perform the sql updates in batches
                if sql_update_count >= sql_update_max_count:
                    # print(sql_cmds)
                    self.dbclient.execute_sql_query(sql_cmds)   # todo: add try block
                    sql_cmds = ''
                    sql_update_count = 0

            # perform any remaining updates
            if len(sql_cmds) > 0:
                self.dbclient.execute_sql_query(sql_cmds)   # todo: add try block

    def merge_sql_deletes(self):
        pass

    def get_column_id(self, column_name):
        """Returns a column id for a given column name"""
        column_map = dict()
        for column in self.sheet.columns:
            column_map[column.title] = column.id
        self.id = column_map[column_name]
        return self.id


if __name__ == '__main__':
    # SQL Params:
    server_name = 'SAC-DATAANLTC'
    database_name = 'BI_ProfServices'
    # database_name = 'BI_TEST'
    schema_name = 'smartsheet'
    table_name = 'CARE_SL Carousels EOS assets_upload'
    # table_name = 'SS Integration Test'

    # Smartsheet Params:
    access_token = os.environ['SS_TOKEN_BRIAN']
    sheet_id = '6080503744515972'
    # sheet_id = '3594598685298564'                   # SS Integration Test
    primary_key = 'Asset Id'

    columns_config = {
        'EOService Group': {'type': 'PUSH', 'sql': 'EOService_Group'},
        # 'EOService  Date': {'type': 'PUSH', 'sql': 'EOService_Date'},
        # 'Primary Contract End Date': {'type': 'PUSH', 'sql': 'Primary_Contract_End_Date'},
        'Primary Contract Type': {'type': 'PUSH', 'sql': 'Primary_Contract_Type'},
        # 'Asset Status': {'type': 'PUSH', 'sql': 'Asset_Status'},
        'Requested T&M status': {'type': 'PUSH', 'sql': 'Requested_TM_status'},
        'PO Received for T&M': {'type': 'PUSH', 'sql': 'PO_Received_for_TM'},
        # 'Service Renewals Specialist ': {'type': 'PUSH', 'sql': 'Service_Renewals_Specialist'},
        'Top Level Parent Account': {'type': 'PUSH', 'sql': 'Top_Level_Parent_Account'},
        'Customer Name': {'type': 'PUSH', 'sql': 'Customer_Name'},
        'CSN': {'type': 'PUSH'},
        'Asset Description': {'type': 'PUSH', 'sql': 'Asset_Description'},
        'Serial Number': {'type': 'PUSH', 'sql': 'Serial_Number'},
        'Model ID': {'type': 'PUSH', 'sql': 'Model_ID'},
        'Asset Id': {'type': 'PRIMARY KEY', 'sql': 'Asset_Id'},
        # 'Sales Representative ': {'type': 'PUSH', 'sql': 'Sales_Representative'},
        'Quote Created': {'type': 'PUSH', 'sql': 'Quote_Created'},
        'Quote Number': {'type': 'PUSH', 'sql': 'Quote_Number'},
        'Status': {'type': 'PUSH'},
        # 'Expected Deal Close Date': {'type': 'PUSH', 'sql': 'Expected_Deal_Close_Date'},
        # 'Future Kardex Install Date': {'type': 'PUSH', 'sql': 'Future_Kardex_Install_Date'},
        'SW Platform': {'type': 'PUSH', 'sql': 'SW_Platform'},
        'IOS customer? ': {'type': 'PUSH', 'sql': 'IOS_customer'},
        'CSM review of downtime document': {'type': 'PUSH', 'sql': 'CSM_review_of_downtime_document'},
        'Note': {'type': 'PUSH'},
        # 'SL Install Date': {'type': 'PUSH', 'sql': 'SL_Install_Date'},
        'Primary Contract ID': {'type': 'PUSH', 'sql': 'Primary_Contract_ID'},
    }

    ix = SmartsheetIntegration(access_token=access_token, sheet_id=sheet_id, server_name=server_name,
                               database_name=database_name, schema_name=schema_name,
                               table_name=table_name, columns_config=columns_config)
    ix.print_sheet_column_details(sheet_id=sheet_id)
    # exit()
    ix.merge_smartsheet_inserts()
    # ix.merge_smartsheet_updates()
    # ix.merge_sql_updates()
    # ix.reload_table(_SHEET_ID, schema_name='smartsheet', table_name='QaFieldTacTicketCompliance_Download')
