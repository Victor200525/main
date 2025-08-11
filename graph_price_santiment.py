import pandas as pd
import matplotlib.pyplot as plt

# Читаем parquet
df = pd.read_parquet("BTC_sentiment_table.parquet")

# Приводим дату и делаем её индексом
df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
df = df.set_index('Date')

# Ресемплинг по 7 дням
df_weekly = df.resample('7D').mean().reset_index()

# График
fig, ax1 = plt.subplots(figsize=(12,6))

ax1.plot(df_weekly['Date'], df_weekly['Close'], color='blue', label='Close Price')
ax1.set_xlabel('Date')
ax1.set_ylabel('Close Price', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()
ax2.plot(df_weekly['Date'], df_weekly['avg_weight_balanced'], color='orange', label='Avg Weight Balanced')
ax2.set_ylabel('Avg Weight Balanced', color='orange')
ax2.tick_params(axis='y', labelcolor='orange')

plt.title('Close и Avg Weight Balanced (недельное усреднение)')
ax1.grid(True)
fig.tight_layout()
plt.show()
