import yfinance as yf
import pandas as pd
import config

def eod_btc_quotes():
    OUTPUT_DIR = config.OUTPUT_DIR
    # Загружаем данные
    data = yf.download("BTC-USD", start="2014-09-17", end="2025-08-10")

    # Оставляем только колонку 'Close' и сбрасываем индекс, чтобы 'Date' была колонкой
    #close_data = data.reset_index()[['Date', 'Close']]
    #close_data = data[['Close']].reset_index()
    close_data = data['Close'].reset_index()  # работает, если 'Close' доступен напрямую
    close_data.columns = ['Date', 'Close']   # переименовываем, если нужно

    # Преобразуем столбец 'Date' к формату только даты (без времени)
    close_data['Date'] = close_data['Date'].dt.date

    # Конвертируем в Polars DataFrame
    #pl_df = pl.from_pandas(close_data)

    # Сохраняем в Parquet
    #pl_df.write_parquet("BTC-USD-close.parquet")
    #pl_df.write_parquet(f"{OUTPUT_DIR}/BTC-USD-close.parquet") 
    close_data.to_parquet(f"{OUTPUT_DIR}/BTC-USD-close.parquet", index=False)

    print("Данные сохранены в BTC-USD-close.parquet")

    return close_data


