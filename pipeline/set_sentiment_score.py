import json
from datetime import datetime
import pandas as pd
import math
import os
import config
import requests  # для Hugging Face API
from core.sentiment import SentimentHuggingFace, SentimentNTLK

def set_sentiment():
    INPUT_DIR = config.INPUT_DIR
    OUTPUT_DIR = config.STAGE_DIR
    # ==== Hugging Face API ====
    BASE_URL = "https://gxdy-work.hf.space"
    API_KEY = "value1"
    HEADERS = {"x-api-key": API_KEY}

    sentiment_model = SentimentHuggingFace(base_url=BASE_URL, api_key=API_KEY)
    #sentiment_model = SentimentNTLK()

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

                        #УБРАННАЯ NLTK

                        # ==== Вызов Hugging Face API и вывод результата ====
                        #hf_sentiment = get_santiment(selftext)
                        #print(f"[{filename} | Line {line_number}] HF Sentiment: {hf_sentiment}")
                        sentiment_score = sentiment_model.get_sentiment(selftext)

                        rows.append({
                            'text': selftext,
                            'upvotes': upvotes,
                            'numofcomms': num_of_comments,
                            'sentiment': sentiment_score,
                            'Date': str(date),
                        })

                    except json.JSONDecodeError as e:
                        print(f"[{filename} | Line {line_number}] JSON ошибка: {e}")
                    except Exception as e:
                        print(f"[{filename} | Line {line_number}] Неожиданная ошибка: {e}")

            if rows:
                import polars as pl
                df = pl.DataFrame(rows)
                #df.write_delta(OUTPUT_DIR, mode="append")
                #print(f"✅ {filename} → Delta Lake ({OUTPUT_DIR})")
                print(df)

        except FileNotFoundError:
            print(f"Файл {filename} не найден.")
        except Exception as e:
            print(f"Ошибка чтения {filename}: {e}")
