import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd
import math
import os
import config
import requests  # для Hugging Face API

# ==== Hugging Face API ====
BASE_URL = "https://gxdy-work.hf.space"
API_KEY = "value1"
HEADERS = {"x-api-key": API_KEY}

def get_santiment(text: str) -> int:
    """Отправляет текст в Hugging Face API и возвращает +1 / -1 / 0"""
    try:
        payload = {"text": text}
        response = requests.post(f"{BASE_URL}/analyze", json=payload, headers=HEADERS)
        result = response.json()
        label = result["result"][0]["label"].lower()

        if label == "positive":
            return 1
        elif label == "negative":
            return -1
        else:
            return 0
    except Exception as e:
        print(f"Ошибка API: {e}")
        return 0

def set_sentiment():
    INPUT_DIR = config.INPUT_DIR
    OUTPUT_DIR = config.STAGE_DIR

    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()

    for filename in os.listdir(INPUT_DIR):
        file_path = os.path.join(INPUT_DIR, filename)

        if not os.path.isfile(file_path):
            continue

        rows = []

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line_number, line in enumerate(file, 1):
                    try:
                        data = json.loads(line)
                        selftext = data.get('selftext', '')
                        upvotes = data.get('score', 0)
                        date_created = data.get('created_utc', None)
                        num_of_comments = data.get('num_comments', 0)

                        if not selftext.strip() or selftext.strip().lower() in {"[deleted]", "[removed]"}:
                            continue

                        try:
                            date = datetime.utcfromtimestamp(int(date_created)).date()
                        except Exception as e:
                            print(f"[{filename} | Line {line_number}] Ошибка даты: {e}")
                            continue

                        try:
                            sentiment = sia.polarity_scores(selftext)
                        except Exception as e:
                            print(f"[{filename} | Line {line_number}] Ошибка анализа тональности: {e}")
                            sentiment = {}

                        compound = sentiment.get('compound', 0)
                        norm_score_log = math.log1p(max(upvotes, 0))
                        norm_score_0_1 = norm_score_log / math.log1p(max(1, upvotes))
                        weight_balanced = 0.5 * norm_score_0_1 + 0.5 * ((compound + 1) / 2)

                        # ==== Вызов Hugging Face API и вывод результата ====
                        hf_sentiment = get_santiment(selftext)
                        print(f"[{filename} | Line {line_number}] HF Sentiment: {hf_sentiment}")

                        rows.append({
                            'text': selftext,
                            'upvotes': upvotes,
                            'numofcomms': num_of_comments,
                            'sentiment': compound,
                            'HF_sentiment': hf_sentiment,  # сохраняем в таблицу
                            'Date': str(date),
                            'weight_balanced': weight_balanced,
                        })

                    except json.JSONDecodeError as e:
                        print(f"[{filename} | Line {line_number}] JSON ошибка: {e}")
                    except Exception as e:
                        print(f"[{filename} | Line {line_number}] Неожиданная ошибка: {e}")

            if rows:
                import polars as pl
                df = pl.DataFrame(rows)
                df.write_delta(OUTPUT_DIR, mode="append")
                print(f"✅ {filename} → Delta Lake ({OUTPUT_DIR})")

        except FileNotFoundError:
            print(f"Файл {filename} не найден.")
        except Exception as e:
            print(f"Ошибка чтения {filename}: {e}")
