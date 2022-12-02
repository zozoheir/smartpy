import ast
import gspread
from smartpy.quant.arithmetic import isNumber


class GoogleSheets:

    def __init__(self, service_account_key_path, spreadsheet_key):
        gs = gspread.service_account(filename=service_account_key_path)
        self.spreadsheet = gs.open_by_key(spreadsheet_key)

    def getRangeAsLists(self, worksheet_name, range_name):
        return self.spreadsheet.worksheet(worksheet_name).get(range_name)

    def getWorksheetAsDict(self, worksheet_name, range_name):
        lists = self.getRangeAsLists(worksheet_name, range_name)
        params_dic = {i[0]: self._processCellValue(i[1]) for i in lists}
        return params_dic

    def getCell(self, worksheet_name, cell_name):
        cell_value = self.spreadsheet.worksheet(worksheet_name).get(cell_name)
        cell_value = str(cell_value[0][0])
        return self._processCellValue(cell_value)

    def put(self, worksheet_name, cell, value):
        self.spreadsheet.worksheet(worksheet_name).requestBalances(cell, str(value))

    @staticmethod
    def _processCellValue(cell_value):
        if isNumber(cell_value):
            return float(cell_value)
        elif cell_value.startswith('[') and cell_value.endswith(']'):
            return ast.literal_eval(cell_value)
        else:
            return cell_value



