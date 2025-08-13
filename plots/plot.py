'''
просто пример вызова для рисования чарта. График сохраняется в plot.html
'''
from utils.plot_echarts import PlotDateEcharts

import pandas as pd


# Тестовые данные
data = {
    'date_': pd.date_range(start='2023-01-01', periods=10),  # Даты
    'value_': [10.5, 12.3, 11.7, 14.2, 15.0, 13.8, 16.1, 17.5, 18.0, 19.2],  # Произвольные значения
    'close_': [9.8, 11.2, 10.9, 13.5, 14.3, 12.7, 15.0, 16.4, 17.1, 18.5]    # Второй ряд значений
}

df = pd.DataFrame(data)
get_plot = PlotDateEcharts()
x_data = df['date_'].tolist()
y1_data = df['value_'].round(2).tolist()
y2_data = df['close_'].round(2).tolist()

get_plot.plotLine_2(x_data, y1_data, y2_data) # создаст файл plot.html


