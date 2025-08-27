import pandas as pd
import config

def join_sentiment_price(sentiment, btc):
    OUTPUT_DIR = config.OUTPUT_DIR
    # Загружаем BTC
    #btc = pd.read_parquet("BTC-USD-close.parquet").copy()
    #btc.columns = ["Date", "Close"]  # нормализуем имена колонок

    # Загружаем сентимент
    #sentiment = pd.read_parquet("aggregated_weights.parquet").copy()
    #btc = btc.to_pandas()
    sentiment = sentiment.to_pandas()
    #btc = btc.reset_index()
    #sentiment = sentiment.reset_index()
    # Приводим столбцы Date к datetime
    btc['Date'] = pd.to_datetime(btc['Date'])
    sentiment['Date'] = pd.to_datetime(sentiment['Date'])

    # Объединяем по дате
    merged = pd.merge(btc, sentiment, on="Date", how="inner")

    # Сохраняем в Parquet
    # merged.to_parquet("BTC_sentiment_table.parquet", index=False)
    merged.to_parquet(f"{OUTPUT_DIR}/BTC_sentiment_table.parquet", index=False)  # 3. Сохраняем

    print("Данные сохранены в BTC_sentiment_table.parquet")
