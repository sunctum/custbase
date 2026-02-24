import streamlit as st
import pandas as pd
import nltk
import pymorphy2
from nltk.corpus import stopwords
from collections import Counter, defaultdict
import re
import os

# Загрузка стоп-слов
stop_words = set(stopwords.words("russian"))

# Морфоанализатор
morph = pymorphy2.MorphAnalyzer()


@st.cache_data
def load_data(file_path="full_name_tagging_base.xlsx"):
    df = pd.read_excel(file_path)
    return df["prod_details"].dropna().tolist()


@st.cache_data
def preprocess_words(product_names):
    words = []
    context_dict = defaultdict(list)
    for name in product_names:
        tokens = re.findall(r"\b[а-яА-Яa-zA-Z]+\b", name.lower())
        for token in tokens:
            if token not in stop_words and len(token) > 2:
                lemma = morph.parse(token)[0].normal_form
                words.append(lemma)
                if len(context_dict[lemma]) < 3:
                    context_dict[lemma].append(name)
    return Counter(words), context_dict


def init_state(word_list, word_counts, context_dict):
    if "approved" not in st.session_state:
        st.session_state.approved = set()
    if "rejected" not in st.session_state:
        st.session_state.rejected = set()
    if "page" not in st.session_state:
        st.session_state.page = 0
    if "words_per_page" not in st.session_state:
        st.session_state.words_per_page = 100
    if "word_list" not in st.session_state:
        st.session_state.word_list = word_list
    if "word_counts" not in st.session_state:
        st.session_state.word_counts = word_counts
    if "context_dict" not in st.session_state:
        st.session_state.context_dict = context_dict
    if "word_votes" not in st.session_state:
        st.session_state.word_votes = {}
    if os.path.exists("tagged_words.csv"):
        df_prev = pd.read_csv("tagged_words.csv")
        st.session_state.approved = set(df_prev[df_prev["tag"] == "approved"]["word"])
        st.session_state.rejected = set(df_prev[df_prev["tag"] == "rejected"]["word"])


def word_batch():
    start = st.session_state.page * st.session_state.words_per_page
    end = start + st.session_state.words_per_page
    return st.session_state.word_list[start:end]


def main():
    st.title("🔍 Классификация слов: одобренные / исключения")
    product_names = load_data()
    freq, context_dict = preprocess_words(product_names)
    word_list = [word for word, _ in freq.most_common()]

    init_state(word_list, freq, context_dict)

    st.write(f"📄 Страница {st.session_state.page + 1} из {len(st.session_state.word_list) // st.session_state.words_per_page + 1}")

    show_only_unclassified = st.checkbox("👁️ Показывать только непомеченные слова")

    if show_only_unclassified:
        filtered_words = [w for w in word_list if w not in st.session_state.approved and w not in st.session_state.rejected]
    else:
        filtered_words = word_list

    filtered_words = sorted(filtered_words, key=lambda w: st.session_state.word_counts[w], reverse=True)
    st.session_state.word_list = filtered_words

    visible_words = []

    with st.form("word_form"):
        for word in word_batch():
            visible_words.append(word)

            col1, col2 = st.columns([2, 6])
            with col1:
                st.markdown(f"**{word}** ({st.session_state.word_counts[word]} упоминаний)")
            with col2:
                current_vote = st.session_state.word_votes.get(word)
                if not current_vote:
                    if word in st.session_state.approved:
                        current_vote = "Одобрено"
                    elif word in st.session_state.rejected:
                        current_vote = "Исключено"
                    else:
                        current_vote = "Не выбрано"

                choice = st.radio(
                    f" ", ["Не выбрано", "Одобрено", "Исключено"],
                    key=f"radio_{word}",
                    index=["Не выбрано", "Одобрено", "Исключено"].index(current_vote),
                    horizontal=True,
                    label_visibility="collapsed"
                )
                st.session_state.word_votes[word] = choice

            with st.expander("Контекст"):
                for example in st.session_state.context_dict.get(word, []):
                    st.write(f"- {example}")

        submitted = st.form_submit_button("💾 Сохранить выбор")

    if submitted:
        for word in visible_words:
            choice = st.session_state.word_votes.get(word, "Не выбрано")
            if choice == "Одобрено":
                st.session_state.approved.add(word)
                st.session_state.rejected.discard(word)
            elif choice == "Исключено":
                st.session_state.rejected.add(word)
                st.session_state.approved.discard(word)
            else:
                st.session_state.rejected.discard(word)
                st.session_state.approved.discard(word)

        # Чистим только обработанные слова
        for word in visible_words:
            st.session_state.word_votes.pop(word, None)

        df_autosave = pd.DataFrame({
            "word": list(st.session_state.approved | st.session_state.rejected),
            "tag": ["approved" if w in st.session_state.approved else "rejected"
                    for w in (st.session_state.approved | st.session_state.rejected)]
        })
        df_autosave.to_csv("tagged_words.csv", index=False, encoding="utf-8")
        st.success("💾 Прогресс сохранён во временный файл!")

    col_prev, col_next = st.columns(2)
    with col_prev:
        if st.button("⬅️ Назад", disabled=st.session_state.page == 0):
            st.session_state.page -= 1
    with col_next:
        if st.button("➡️ Следующий набор", disabled=(st.session_state.page + 1) * st.session_state.words_per_page >= len(st.session_state.word_list)):
            st.session_state.page += 1

    with st.expander("📊 Посмотреть текущие результаты"):
        st.write("✅ Одобренные:")
        st.write(sorted(st.session_state.approved))
        st.write("🚫 Исключенные:")
        st.write(sorted(st.session_state.rejected))

    with st.expander("📤 Экспорт в Excel"):
        if st.button("💾 Скачать как Excel"):
            df_out = pd.DataFrame({
                "word": list(st.session_state.approved | st.session_state.rejected),
                "tag": ["approved" if w in st.session_state.approved else "rejected"
                        for w in (st.session_state.approved | st.session_state.rejected)]
            })
            st.download_button(
                label="📥 Скачать",
                data=df_out.to_csv(index=False).encode("utf-8"),
                file_name="tagged_words.csv",
                mime="text/csv"
            )


if __name__ == "__main__":
    main()
