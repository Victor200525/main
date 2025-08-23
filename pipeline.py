from pipeline.join_sentiment_price import join_sentiment_price
from pipeline.download_yh_price_data import eod_btc_quotes
from pipeline.set_sentiment_score import set_sentiment
from pipeline.group_by_date import group_by_date
import plots.plot as plt
import config

STAGE_DIR = config.STAGE_DIR
new_data = False # просто индикатор нам обрабатывать новые данные или анализировать уже имеющиеся

# step-by-step
# вызываем функции обратобки из каталога pipeline 

if new_data: 
    await set_sentiment() # parsing json file, save to delta STAGING table
#btc_quotes = eod_btc_quotes() # get quotes from Yahoo
#df_sentiment = group_by_date()
#join_sentiment_price(df_sentiment, btc_quotes) # JOIN dataframes
#plt.plot_data()
