import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd




sia = SentimentIntensityAnalyzer()

rows = []

with open('check', 'r', encoding='utf-8') as file:
    for line in file:
        data = json.loads(line)
        selftext = data.get('selftext', '')
        upvotes = data.get('score', '')
        date_created = data.get('created_utc', '')
        num_of_comments = data.get('num_comments', '')
        date = datetime.utcfromtimestamp(date_created).date()
        sentiment = sia.polarity_scores(selftext)
        #print('Text:', selftext, 'Upvotes:', upvotes, 'Number of Comments:', num_of_comments, 'Sentiment:', sentiment, 'Date:', date)
        #print('=====================')
        rows.append({
            'text': selftext,
            'upvotes': upvotes,
            'numofcomms': num_of_comments,
            'sentiment': sentiment,
            'date': date
        })
df = pd.DataFrame(rows)
df.to_parquet('output.parquet', engine='pyarrow', index=False)
