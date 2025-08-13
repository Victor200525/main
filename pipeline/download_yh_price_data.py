import yfinance as yf
import polars as pl

def eod_btc_quotes():
    # Загружаем данные
    data = yf.download("BTC-USD", start="2014-09-17", end="2025-08-10")

    # Оставляем только колонку 'Close' и сбрасываем индекс, чтобы 'Date' была колонкой
    close_data = data[['Close']].reset_index()

    # Преобразуем столбец 'Date' к формату только даты (без времени)
    close_data['Date'] = close_data['Date'].dt.date

    # Конвертируем в Polars DataFrame
    pl_df = pl.from_pandas(close_data)

    # Сохраняем в Parquet
    pl_df.write_parquet("BTC-USD-close.parquet")

    print("Данные сохранены в BTC-USD-close.parquet")
