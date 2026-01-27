# utils/io.py

import pandas as pd

def read_excel_file(path, sheet_name=0) -> pd.DataFrame:
    """Чтение Excel-файла в DataFrame."""
    return pd.read_excel(
        path, 
        sheet_name
    )

def save_to_excel_file(df: pd.DataFrame, path: str, index=False):
    """Сохранение DataFrame в Excel-файл."""
    df.to_excel(path, index=index)