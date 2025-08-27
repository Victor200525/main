'''
График сохраняется в plot.html
'''
import pandas as pd
from plots.plot_echarts import PlotDateEcharts # это наш класс
import config

def plot_data():
    OUTPUT_DIR = config.OUTPUT_DIR

    # Чтение Parquet-файла
    df = pd.read_parquet(f"{OUTPUT_DIR}/BTC_sentiment_table.parquet", engine='pyarrow')
    df['avg_weight_balanced'] = df['avg_weight_balanced'].rolling(7).mean()
    get_plot = PlotDateEcharts()

    x_data = df['Date'].tolist()
    y1_data = df['avg_weight_balanced'].round(2).tolist()
    y2_data = df['Close'].round(2).tolist()

    get_plot.plotLine_2(x_data, y1_data, y2_data) # создаст файл plot.html

def plot_summary_data(): # распечатать итог из файла df_sentiment_data.csv
    OUTPUT_DIR = config.OUTPUT_DIR
    df = pd.read_csv(OUTPUT_DIR + '/df_sentiment_score.csv')
    get_plot = PlotDateEcharts()
    x_data = df['date_'].tolist()
    y1_data = df['avg_score'].round(2).tolist()
    y2_data = df['close_'].round(2).tolist()
    get_plot.plotLine_2(x_data, y1_data, y2_data) # создаст файл plot.html
