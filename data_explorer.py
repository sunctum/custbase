import pandas as pd
import numpy as np


# Загрузка данных (путь можно заменить на нужный)
file_path = 'data/st3_enriched/st3.xlsx'
df = pd.read_excel(file_path)

mask = (
    (df['is_blacklisted_manual'] == False) &
    (df['is_bad_exporter'] == False) &
    (df['is_bad_importer'] == False) &
    (df['is_valid'] == True) &
    (
        (df['classification'] == 'одобрено') |
        (df['classification'].isna())
    )
)

filtered_df = df[mask]

filtered_df.to_excel('data/check.xlsx', index=False)