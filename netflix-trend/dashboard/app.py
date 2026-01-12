import dash
from dash import dcc, html
import plotly.express as px
from pymongo import MongoClient
import pandas as pd
from dash.dependencies import Input, Output

# Kết nối MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["netflix_trend"]
collection = db["clean_search_logs"]

def fetch_data():
    data = list(collection.find())
    df = pd.DataFrame(data) # Chuyển đổi dữ liệu từ MongoDB sang DataFrame
    return df

app = dash.Dash(__name__) # Khởi tạo ứng dụng Dash

app.layout = html.Div([ #sd t con cua dash de tao cac biểu đồ
    html.H1("Netflix Search Trend Dashboard"),
    dcc.Interval(id='interval', interval=5000, n_intervals=0),  # Tự động cập nhật mỗi 5 giây
    dcc.Graph(id='genre-bar'), # Biểu đồ thể loại
    dcc.Graph(id='region-pie'), # Biểu đồ vùng
    dcc.Graph(id='device-bar')  , # Biểu đồ thiết bị
    dcc.Graph(id='region-trend-bar')  # Biểu đồ thể loại theo vùng
])

@app.callback(
    [Output('genre-bar', 'figure'),
     Output('region-pie', 'figure'),
     Output('device-bar', 'figure'),
     Output('region-trend-bar', 'figure')],  #
    [Input('interval', 'n_intervals')]
)
def update_graphs(n):
    df = fetch_data()
    if df.empty:
        return {}, {}, {}
    genre_counts = df['genre'].value_counts().reset_index()
    genre_counts.columns = ['genre', 'count']
    genre_fig = px.bar(genre_counts,
                       x='genre', y='count',
                       labels={'genre': 'Genre', 'count': 'Search Count'},
                       title='Top Genres Searched')
    
    region_fig = px.pie(df, names='region', title='Searches by Region')
    
    device_counts = df['device'].value_counts().reset_index()
    device_counts.columns = ['device', 'count']
    device_fig = px.bar(device_counts,
                        x='device', y='count',
                        labels={'device': 'Device', 'count': 'Search Count'},
                        title='Searches by Device')

    # === Thêm: Thể loại trend theo từng vùng ===
    trend_df = df.groupby(['region', 'genre']).size().reset_index(name='count')
    idx = trend_df.groupby('region')['count'].idxmax()
    region_trend = trend_df.loc[idx][['region', 'genre', 'count']]

    trend_fig = px.bar(region_trend, x='region', y='count', color='genre',
                       title='Trending Genre by Region',
                       labels={'count': 'Search Count', 'genre': 'Trending Genre'})

    return genre_fig, region_fig, device_fig, trend_fig

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)