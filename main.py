import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd
import math #check

# Загружаем словарь для Vader
nltk.download('vader_lexicon', quiet=True)
sia = SentimentIntensityAnalyzer()

rows = []

try:
    with open('Bitcoin_submissions', 'r', encoding='utf-8') as file:
        for line_number, line in enumerate(file, 1):
            try:
                data = json.loads(line)
                selftext = data.get('selftext', '')
                upvotes = data.get('score', 0)
                date_created = data.get('created_utc', None)
                num_of_comments = data.get('num_comments', 0)

                # Пропускаем пустые, удалённые и модератором удалённые посты
                if not selftext.strip() or selftext.strip().lower() in {"[deleted]", "[removed]"}:
                    continue

                try:
                    date = datetime.utcfromtimestamp(int(date_created)).date()
                except Exception as e:
                    print(f"[Line {line_number}] Error parsing timestamp: {e}")
                    continue

                try:
                    sentiment = sia.polarity_scores(selftext)
                except Exception as e:
                    print(f"[Line {line_number}] Sentiment analysis error: {e}")
                    sentiment = {}

                compound = sentiment.get('compound', 0)

                # Нормализуем score
                norm_score_log = math.log1p(max(upvotes, 0))  # логарифм
                norm_score_0_1 = norm_score_log / math.log1p( max(1, upvotes) )  # грубая нормализация (можно заменить на глобальный max)


                # 2. Сбалансированный
                weight_balanced = 0.5 * norm_score_0_1 + 0.5 * ((compound + 1) / 2)



                rows.append({
                    'text': selftext,
                    'upvotes': upvotes,
                    'numofcomms': num_of_comments,
                    'sentiment': compound,
                    'Date': date,
                    'weight_balanced': weight_balanced,
                })

            except json.JSONDecodeError as e:
                print(f"[Line {line_number}] JSON decode error: {e}")
            except Exception as e:
                print(f"[Line {line_number}] Unexpected error: {e}")

except FileNotFoundError:
    print("File 'Bitcoin_submissions' not found.")
except Exception as e:
    print(f"Error reading file: {e}")

# Сохраняем результат
if rows:
    try:
        df = pd.DataFrame(rows)
        df.to_parquet('output.parquet', engine='pyarrow', index=False)
        print("Data successfully saved to 'output.parquet'.")
    except Exception as e:
        print(f"Error saving DataFrame to Parquet: {e}")
else:
    print("No data collected; output file not created.")
