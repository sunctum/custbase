# utils/io.py

import pandas as pd

def read_excel_file(path, sheet_name='Sheet1', parse_dates=None) -> pd.DataFrame:
    """Чтение Excel-файла в DataFrame."""
    return pd.read_excel(path, sheet_name=sheet_name, parse_dates=parse_dates)

def save_to_excel_file(df: pd.DataFrame, path: str, index=False):
    """Сохранение DataFrame в Excel-файл."""
    df.to_excel(path, index=index)