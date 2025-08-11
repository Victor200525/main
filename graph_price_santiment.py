import pandas as pd
import matplotlib.pyplot as plt

# Читаем parquet
df = pd.read_parquet("BTC_sentiment_table.parquet")

# Приводим дату к datetime и нормализуем (на всякий случай)
df['Date'] = pd.to_datetime(df['Date']).dt.normalize()

# Строим график с двумя осями Y
fig, ax1 = plt.subplots(figsize=(12,6))

# Первая ось Y для Close
ax1.plot(df['Date'], df['Close'], color='blue', label='Close Price')
ax1.set_xlabel('Date')
ax1.set_ylabel('Close Price', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

# Вторая ось Y для avg_weight_balanced
ax2 = ax1.twinx()
ax2.plot(df['Date'], df['avg_weight_balanced'], color='orange', label='Avg Weight Balanced')
ax2.set_ylabel('Avg Weight Balanced', color='orange')
ax2.tick_params(axis='y', labelcolor='orange')

# Заголовок и сетка
plt.title('График Close и Avg Weight Balanced')
ax1.grid(True)
fig.tight_layout()
plt.show()
