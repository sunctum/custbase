# steps/step2_tagging.py

from datetime import datetime
import pandas as pd
import re
import pymorphy2
from nltk import download
from nltk.corpus import stopwords

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

logger = setup_logger()
start_time = datetime.now()
logger.info('--- Step 2: Текстовая классификация ---')

# --- Пути ---
INPUT_PATH = 'data/st1_cleaned/st1.xlsx'
OUTPUT_PATH = 'data/st2_tagged/st2.xlsx'
TAGS_PATH = 'data/utilities/word_tagger/tagged_words.csv'

# --- Загрузка ресурсов ---
morph = pymorphy2.MorphAnalyzer()
download('stopwords')
stop_words = set(stopwords.words("russian"))

# --- Загрузка данных ---
df = read_excel_file(INPUT_PATH)
tags_df = pd.read_csv(TAGS_PATH)
approved = set(tags_df[tags_df["tag"] == "approved"]["word"].str.lower())
rejected = set(tags_df[tags_df["tag"] == "rejected"]["word"].str.lower())

# --- Обработка текста ---
def extract_lemmas(text: str) -> list[str]:
    tokens = re.findall(r"\b[а-яА-Яa-zA-Z]+(?:-[а-яА-Яa-zA-Z]+)*\b", text.lower())
    lemmas = []
    for token in tokens:
        if token not in stop_words and len(token) > 1:
            lemma = morph.parse(token)[0].normal_form
            lemmas.append(lemma)
    return lemmas

def classify_text(text: str):
    if pd.isna(text):
        return "не определено", "", [], []

    lemmas = extract_lemmas(text)
    matched_approved = [l for l in lemmas if l in approved]
    matched_rejected = [l for l in lemmas if l in rejected]

    if matched_rejected:
        return "исключено", matched_rejected[0], matched_approved, matched_rejected
    elif matched_approved:
        return "одобрено", matched_approved[0], matched_approved, matched_rejected
    else:
        return "не определено", "", matched_approved, matched_rejected

# --- Применение классификации ---
results = df["prod_details"].apply(classify_text)
df["classification"] = results.str[0]
df["reason"] = results.str[1]
df["matched_approved"] = results.str[2]
df["matched_rejected"] = results.str[3]

# --- Сохранение ---
save_to_excel_file(df, OUTPUT_PATH)

end_time = datetime.now()
logger.info(f'Время начала: {start_time}')
logger.info(f'Время окончания: {end_time}')
logger.info(f'Продолжительность: {end_time - start_time}')
logger.info(f'Готово: {OUTPUT_PATH}')