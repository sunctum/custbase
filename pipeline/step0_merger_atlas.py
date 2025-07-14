import pandas as pd
import glob
import os

# Папка с файлами
folder_path = r"C:\Users\424\Documents\custbase\data\raw\atlas"  
file_list = glob.glob(os.path.join(folder_path, "*.xlsx"))

combined_df_list = []

for file in file_list:
    # Чтение первого листа, пропуская первую строку
    df = pd.read_excel(file, sheet_name=0, skiprows=1)
    df['source_file'] = os.path.basename(file)  
    combined_df_list.append(df)

# Объединение всех датафреймов
combined_df = pd.concat(combined_df_list, ignore_index=True)

if "EXPORTER COUNTRY" in combined_df.columns:
    combined_df = combined_df[combined_df["EXPORTER COUNTRY"] != "Russian Federation"]
else:
    raise ValueError("Столбец 'EXPORTER COUNTRY' не найден в данных")

# Удаление дубликатов по всем столбцам, кроме "NO"
cols_to_check = [col for col in combined_df.columns if col != "NO"]
duplicated_mask = combined_df.duplicated(subset=cols_to_check, keep=False)

# Лог дубликатов (включает все повторы, не только те, которые удаляются)
duplicates_df = combined_df[duplicated_mask].copy()

# Удаляем дубликаты, оставляя первое вхождение
deduplicated_df = combined_df.drop_duplicates(subset=cols_to_check, keep='first')

# Сохраняем результат в Excel с двумя листами
with pd.ExcelWriter(r'C:\Users\424\Documents\custbase\data\raw\atlas.xlsx', engine="openpyxl") as writer:
    deduplicated_df.to_excel(writer, sheet_name="Данные", index=False)
    duplicates_df.to_excel(writer, sheet_name="Дубликаты", index=False)