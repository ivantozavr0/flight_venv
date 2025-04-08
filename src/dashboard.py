import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import plotly
import pandas as pd
import json
import datetime
import time
import csv
import logging

# это дашборд. здесь рисуются маршруты всех самолетов за последний час над Черным морем и гистограммы распределения самолетов по авиалиниям и моделям

logging.basicConfig(
    filename='logs/dashboard.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

#цвета для текста и фона
colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

# в csv все хранится в виде строк, так что преобразуем строковую запись массивов чисел (координат маршрута) в массив чисел
def string_to_array(s):
    return json.loads(s)

def create_trail_figure():
# берем информацию за последний час
    global colors
    
    columns = ['icao', 'model', 'trail']
    df_trails = None
    try:
        df_trails = pd.read_csv("data/hourly_report.csv", usecols=['icao', 'model', 'trail'])
    except FileNotFoundError as e:
        df_trails = pd.DataFrame(columns=columns)
    
    # это будет график для маршрутов
    fig = go.Figure()

    #загружаем все доступные plotly краски
    color_palette = plotly.colors.qualitative.Plotly
    counter = 0

    for index, row in df_trails.iterrows():
    # преобразовали строки в масивы чисел
        float_trail = string_to_array(row['trail'])
        # формируем маршрут
        latitudes = [point[0] for point in float_trail]
        longitudes = [point[1] for point in float_trail]

        #добавляем новую линию
        fig.add_trace(go.Scattergeo(
            lat=latitudes,
            lon=longitudes,
            mode='lines',
            line=dict(width=2, color=color_palette[index%len(color_palette)]), # краисм в тот или иной цвет
            name=row['icao'],
            legendgroup=row['model'], # группируем по моделям
            legendgrouptitle_text=row['model']
        ))

    # добавили карту земли
    fig.update_layout(
    showlegend = True,
    geo = dict(
        scope = 'world',
        landcolor = 'rgb(255, 217, 217)',
        bgcolor = 'rgb(0, 255, 255)'
    ),
    plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'],
    title_font_color="white",
    legend_font_color="white",
    font={ 'color': colors['text'] }
)
    return fig

# гистограмма по авиалиниям
def create_airline_figure():
    global colors

    columns = ['airline', 'numb']
    df_air = None

    try:
        df_air = pd.read_csv("data/airline_report.csv")
    except FileNotFoundError as e:
        df_air = pd.DataFrame(columns=columns)
        
    fig = px.bar(df_air, x="numb", y="airline", orientation='h', color="airline")
    fig.update_layout(plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'], font={
                    'color': colors['text']
                })
    return fig

# гистограмма по моделям
def create_model_figure():
    global colors

    columns = ['model', 'numb']
    df_mod = None

    try:
        df_mod = pd.read_csv("data/model_report.csv")
    except FileNotFoundError as e:
        df_mod = pd.DataFrame(columns=columns)

    fig = px.bar(df_mod, x="numb", y="model", orientation='h')
    fig.update_layout(plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'], font={
                    'color': colors['text']
                })
    return fig
    
# обновляем информацию о последнем обновлении дашборда
def create_title():
    update_time = datetime.datetime.now()
    logging.info(f"Последнее обновление: {update_time}")
    return f"Последнее обновление: {update_time}"

# собсна создали борд
app = dash.Dash(__name__)

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(children="Маршруты самолетов в акватории Черного моря", style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    html.H2(id='title', children=create_title(), style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    html.Div(children="""Если вы не видите данные, то это значит, что они еще не загружены с сайта FlightRadar24. 
    Во избежание блокировки, парсинг займет некоторое время.\n Дашборд обновляется каждые 2.5 минуты. Вероятнее всего, через 
    этот промежуток времени на дашборде уже появятся маршруты и статистика в виде диаграмм.""", 
             style={
            'color': 'white'
        }),
    html.H3(children="Все обнаруженные маршруты в акватории Черного моря за последний час (или с начала работы программы)", style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    dcc.Graph(id='trail-graph', figure=create_trail_figure()),
    html.H3(children="Количество самолетов в зависимости от авиалинии в акватории Черного моря за последний час (или с начала работы программы)", style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    dcc.Graph(id='airline-graph', figure=create_airline_figure()),
    html.H3(children="Количество самолетов в зависимости от модели в акватории Черного моря за последний час (или с начала работы программы)", style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    dcc.Graph(id='model-graph', figure=create_model_figure()),
    dcc.Interval(
        id='interval-component',
        interval=2.5 * 60 * 1000  # обновляем дашборд каждые 2.5 минуты
    )
])

# будем периодически обновляться 
@app.callback(
    [Output('title', 'children'),
     Output('trail-graph', 'figure'),
     Output('airline-graph', 'figure'),
     Output('model-graph', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    tit = create_title()
    trail_fig = create_trail_figure()
    airline_fig = create_airline_figure()
    model_fig = create_model_figure()
    return tit, trail_fig, airline_fig, model_fig

if __name__ == '__main__':
    app.run(debug=False, port=8030, host='0.0.0.0')

