import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd
import math
import os
import config

def set_sentiment():
    INPUT_DIR = config.INPUT_DIR
    OUTPUT_DIR = config.STAGE_DIR
    # Создаём папку для Delta Lake, если её нет
    #os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Загружаем словарь для Vader
    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()

    # Перебираем все файлы в папке input
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

                        # Пропуск пустых или удалённых
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

                        # Нормализация score
                        norm_score_log = math.log1p(max(upvotes, 0))
                        norm_score_0_1 = norm_score_log / math.log1p(max(1, upvotes))

                        # Сбалансированный вес
                        weight_balanced = 0.5 * norm_score_0_1 + 0.5 * ((compound + 1) / 2)

                        rows.append({
                            'text': selftext,
                            'upvotes': upvotes,
                            'numofcomms': num_of_comments,
                            'sentiment': compound,
                            'Date': str(date),
                            'weight_balanced': weight_balanced,
                        })

                    except json.JSONDecodeError as e:
                        print(f"[{filename} | Line {line_number}] JSON ошибка: {e}")
                    except Exception as e:
                        print(f"[{filename} | Line {line_number}] Неожиданная ошибка: {e}")

            # Сохраняем в Delta Lake
            if rows:
                import polars as pl
                df = pl.DataFrame(rows)

                # Имя директории Delta — общее, данные будут дописываться
                df.write_delta(OUTPUT_DIR, mode="append")
                print(f"✅ {filename} → Delta Lake ({OUTPUT_DIR})")

            # Удаляем исходный файл
            #os.remove(file_path)
            #print(f"🗑 Удалён {filename}")

        except FileNotFoundError:
            print(f"Файл {filename} не найден.")
        except Exception as e:
            print(f"Ошибка чтения {filename}: {e}")

