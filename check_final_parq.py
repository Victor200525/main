import pandas as pd

# Читаем parquet
df = pd.read_parquet("BTC_sentiment_table.parquet")

# Сохраняем в Excel
df.to_excel("BTC_sentiment_table.xlsx", index=False)

# Читаем Excel для проверки
