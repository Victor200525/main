from pyecharts.faker import Faker
import pyecharts.options as opts
import pyecharts.charts as echarts
from pyecharts.globals import ThemeType, CurrentConfig, NotebookType 
from IPython.display import HTML, display

class PlotDateEcharts:
    def __init__(self):
        pass

    def plotLine_2(self,x_data, y1_data, y2_data):

        # Plot Settings
        CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB

        datazoom_opts=[
            opts.DataZoomOpts(type_='slider', range_start=0, range_end=100),
            opts.DataZoomOpts(type_='inside')
        ]

        # Основная ось Y (левая)
        yaxis_opts = opts.AxisOpts(
            name='Flow',
            min_=0.6,
            max_=0.85,
            position="left"
        )

        # Дополнительная ось Y (правая)
        yaxis_opts2 = opts.AxisOpts(
            name='Price',
            position="right"
        )

        xaxis_opts = opts.AxisOpts(name='Date', splitline_opts=opts.SplitLineOpts(is_show=True))
        toolbox_opts = opts.ToolboxOpts(is_show=True, feature={"dataZoom": {"yAxisIndex": 'none'}, "restore": {}})
        tooltip_opts = opts.TooltipOpts(trigger='axis', axis_pointer_type='shadow')

        plot = (
            echarts.Line(init_opts=opts.InitOpts(theme=ThemeType.WHITE))
            .add_xaxis(x_data)
            .add_yaxis(
                "flow", y1_data, yaxis_index=0,
                is_symbol_show=False,
                linestyle_opts=opts.LineStyleOpts(color="blue"),
                areastyle_opts=opts.AreaStyleOpts(opacity=0.8),
                itemstyle_opts=opts.ItemStyleOpts(color="blue", border_color="#FFF", border_width=1)
            )
            .add_yaxis(
                "price", y2_data, yaxis_index=1,
                is_symbol_show=False,
                linestyle_opts=opts.LineStyleOpts(color="red"),
                label_opts=opts.LabelOpts(is_show=True, position="top", color="red"),
                itemstyle_opts=opts.ItemStyleOpts(color="red", border_color="#fff", border_width=2)
            )
            .extend_axis(yaxis=yaxis_opts2)
            .set_global_opts(
                title_opts=opts.TitleOpts(title=""),
                datazoom_opts=datazoom_opts,
                xaxis_opts=xaxis_opts,
                yaxis_opts=yaxis_opts,
                toolbox_opts=toolbox_opts,
                tooltip_opts=tooltip_opts
            )
        )

        display(HTML(plot.render("plot.html")))

    def plotLine_2_2(self,x_data, y1_data, y2_data):
        # Plot Settings
        CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB
        datazoom_opts=[opts.DataZoomOpts(type_='slider', range_start=0, range_end=100),
                 opts.DataZoomOpts(type_='inside')
             ]
        yaxis_opts = opts.AxisOpts(name = '')
        xaxis_opts = opts.AxisOpts(name = 'Date', splitline_opts=opts.SplitLineOpts(is_show=True))

        toolbox_opts=opts.ToolboxOpts(is_show=True, feature={"dataZoom": {"yAxisIndex": 'none'},"restore": {}})
        tooltip_opts=opts.TooltipOpts(trigger='axis', axis_pointer_type='shadow')
        
        plot = (
        echarts.Line(init_opts=opts.InitOpts(theme=ThemeType.WHITE))
        .add_xaxis(x_data)
        .add_yaxis("flow", y1_data, yaxis_index=0, is_symbol_show=False, 
            linestyle_opts=opts.LineStyleOpts(color="blue"), areastyle_opts=opts.AreaStyleOpts(opacity=0.8),
            itemstyle_opts=opts.ItemStyleOpts(color="blue", border_color="#FFF", border_width=1))
        .add_yaxis("price", y2_data, yaxis_index=1, is_symbol_show=False,
            linestyle_opts=opts.LineStyleOpts(color="red"),
            label_opts=opts.LabelOpts(is_show=True, position="top", color="red"),
            itemstyle_opts=opts.ItemStyleOpts(color="red", border_color="#fff", border_width=2))

        .extend_axis( yaxis=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="") )  )
        .set_global_opts(title_opts=opts.TitleOpts(title=""),
                         datazoom_opts=datazoom_opts, xaxis_opts = xaxis_opts, yaxis_opts = yaxis_opts,
                         toolbox_opts = toolbox_opts, tooltip_opts = tooltip_opts)

        )
        #plot.render_notebook()
        display(HTML(plot.render("./plots/plot.html")))