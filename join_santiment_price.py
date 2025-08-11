import pandas as pd

# Загружаем BTC
btc = pd.read_parquet("BTC-USD-close.parquet").copy()
btc.columns = ["Date", "Close"]  # нормализуем имена колонок

# Загружаем сентимент
sentiment = pd.read_parquet("aggregated_weights.parquet").copy()

# Приводим столбцы Date к datetime
btc['Date'] = pd.to_datetime(btc['Date'])
sentiment['Date'] = pd.to_datetime(sentiment['Date'])

# Объединяем по дате
merged = pd.merge(btc, sentiment, on="Date", how="inner")

# Сохраняем в Parquet
merged.to_parquet("BTC_sentiment_table.parquet", index=False)

print("Данные сохранены в BTC_sentiment_table.parquet")
