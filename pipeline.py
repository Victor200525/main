from pipeline.join_sentiment_price import join_sentiment_price
from pipeline.download_yh_price_data import eod_btc_quotes
from pipeline.set_sentiment_score import set_sentiment
from pipeline.group_by_date import group_by_date

# step-by-step
# вызываем функции обратобки из каталога pipeline 

set_sentiment() # parsing json file, save to delta table
eod_btc_quotes() # get quotes from Yahoo
group_by_date()
join_sentiment_price() # JOIN dataframes 
