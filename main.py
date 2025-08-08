import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd

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

                if date_created is None:
                    raise ValueError("Missing 'created_utc' timestamp")

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

                rows.append({
                    'text': selftext,
                    'upvotes': upvotes,
                    'numofcomms': num_of_comments,
                    'sentiment': sentiment,
                    'date': date
                })
            except json.JSONDecodeError as e:
                print(f"[Line {line_number}] JSON decode error: {e}")
            except Exception as e:
                print(f"[Line {line_number}] Unexpected error: {e}")
except FileNotFoundError:
    print("File 'check' not found.")
except Exception as e:
    print(f"Error reading file: {e}")

# Сохраняем результат, если удалось собрать хоть какие-то данные
if rows:
    try:
        df = pd.DataFrame(rows)
        df.to_parquet('output.parquet', engine='pyarrow', index=False)
        print("Data successfully saved to 'output.parquet'.")
    except Exception as e:
        print(f"Error saving DataFrame to Parquet: {e}")
else:
    print("No data collected; output file not created.")
