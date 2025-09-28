#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块指数K线图可视化仪表板
使用Dash和Plotly创建交互式K线图
"""

import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import os
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc

# 导入v2项目的数据库管理器
from data_management.database_manager import DatabaseManager

# 初始化Dash应用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "板块指数K线图仪表板"

# 创建数据库管理器实例
db_manager = DatabaseManager()

def get_available_indices():
    """获取可用的指数列表"""
    try:
        # 获取所有板块指数（包括原标准板块和细化板块）
        query = """
        SELECT DISTINCT index_code, index_name 
        FROM index_k_daily 
        ORDER BY index_code
        """
        indices_df = db_manager.execute_query(query)
        
        return indices_df
    except Exception as e:
        print(f"获取指数列表失败: {e}")
        return pd.DataFrame()

def get_index_data(index_code, days=30):
    """获取指定指数的历史数据"""
    try:
        # 计算开始日期
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        query = """
        SELECT trade_date, open, high, low, close, volume
        FROM index_k_daily
        WHERE index_code = ? 
        AND trade_date >= ? 
        AND trade_date <= ?
        ORDER BY trade_date
        """
        
        df = db_manager.execute_query(query, (index_code, start_date, end_date))
        
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
        
        return df
    except Exception as e:
        print(f"获取指数数据失败: {e}")
        return pd.DataFrame()

def create_candlestick_chart(df, index_code, index_name, trading_days_only=True, show_volume=False):
    """创建K线图"""
    if df.empty:
        return go.Figure().add_annotation(
            text="没有数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # 根据选项决定是否过滤非交易日
    if trading_days_only:
        # 去除没有行情数据的日期（周末、节假日等）
        # 只保留有实际交易数据的日期
        df_clean = df.dropna(subset=['open', 'high', 'low', 'close'])
        df_clean = df_clean[(df_clean['open'] > 0) & (df_clean['high'] > 0) & 
                           (df_clean['low'] > 0) & (df_clean['close'] > 0)]
    else:
        # 显示所有日期，包括非交易日
        df_clean = df.copy()
    
    if df_clean.empty:
        return go.Figure().add_annotation(
            text="没有有效的交易数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # 创建K线图，使用连续的时间序列
    fig = go.Figure(data=go.Candlestick(
        x=df_clean.index,
        open=df_clean['open'],
        high=df_clean['high'],
        low=df_clean['low'],
        close=df_clean['close'],
        name=index_code,
        increasing_line_color='red',    # 上涨为红色
        decreasing_line_color='green'   # 下跌为绿色
    ))
    
    # 设置x轴
    title_suffix = "连续交易日" if trading_days_only else "所有日期"
    xaxis_title = "交易日" if trading_days_only else "日期"
    xaxis_type = 'category' if trading_days_only else 'date'
    
    fig.update_layout(
        title=f"{index_name} ({index_code}) K线图 - {title_suffix}",
        xaxis_title=xaxis_title,
        yaxis_title="指数点位",
        template="plotly_white",
        height=600,
        showlegend=False,
        xaxis=dict(
            type=xaxis_type,
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        )
    )
    
    # 添加移动平均线
    if len(df_clean) >= 5:
        df_clean['MA5'] = df_clean['close'].rolling(window=5).mean()
        df_clean['MA10'] = df_clean['close'].rolling(window=10).mean()
        
        fig.add_trace(go.Scatter(
            x=df_clean.index,
            y=df_clean['MA5'],
            mode='lines',
            name='MA5',
            line=dict(color='orange', width=2),
            connectgaps=False  # 不连接缺失值
        ))
        
        if len(df_clean) >= 10:
            fig.add_trace(go.Scatter(
                x=df_clean.index,
                y=df_clean['MA10'],
                mode='lines',
                name='MA10',
                line=dict(color='blue', width=2),
                connectgaps=False  # 不连接缺失值
            ))
    
    # 添加成交量作为背景信息（可选）
    if 'volume' in df_clean.columns:
        # 创建次y轴显示成交量
        fig.add_trace(go.Bar(
            x=df_clean.index,
            y=df_clean['volume'],
            name='成交量',
            yaxis='y2',
            opacity=0.3,
            marker_color='lightblue',
            showlegend=False
        ))
        
        # 设置次y轴
        fig.update_layout(
            yaxis2=dict(
                title="成交量",
                overlaying='y',
                side='right',
                showgrid=False
            )
        )
    
    return fig

def create_volume_chart(df, index_code, trading_days_only=True):
    """创建成交量图"""
    if df.empty:
        return go.Figure().add_annotation(
            text="没有数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # 根据选项决定是否过滤非交易日，与K线图保持一致
    if trading_days_only:
        # 去除没有行情数据的日期，与K线图保持一致
        df_clean = df.dropna(subset=['open', 'high', 'low', 'close'])
        df_clean = df_clean[(df_clean['open'] > 0) & (df_clean['high'] > 0) & 
                           (df_clean['low'] > 0) & (df_clean['close'] > 0)]
    else:
        # 显示所有日期，包括非交易日
        df_clean = df.copy()
    
    if df_clean.empty:
        return go.Figure().add_annotation(
            text="没有有效的交易数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # 根据涨跌设置成交量柱状图颜色
    colors = []
    for i in range(len(df_clean)):
        if i == 0:
            colors.append('lightblue')  # 第一个交易日
        else:
            if df_clean.iloc[i]['close'] >= df_clean.iloc[i-1]['close']:
                colors.append('red')    # 上涨为红色
            else:
                colors.append('green')  # 下跌为绿色
    
    fig = go.Figure(data=go.Bar(
        x=df_clean.index,
        y=df_clean['volume'],
        name='成交量',
        marker_color=colors,
        opacity=0.7
    ))
    
    # 设置标题和x轴，与K线图保持一致
    title_suffix = "连续交易日" if trading_days_only else "所有日期"
    xaxis_title = "交易日" if trading_days_only else "日期"
    xaxis_type = 'category' if trading_days_only else 'date'
    
    fig.update_layout(
        title=f"{index_code} 成交量 - {title_suffix}",
        xaxis_title=xaxis_title,
        yaxis_title="成交量",
        template="plotly_white",
        height=300,
        showlegend=False,
        xaxis=dict(
            type=xaxis_type,  # 与K线图保持一致
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        )
    )
    
    return fig

# 获取可用指数数据
indices_df = get_available_indices()

# 创建选项列表
index_options = [{'label': f"{row['index_code']} - {row['index_name']}", 'value': row['index_code']} 
                for _, row in indices_df.iterrows()]

# 应用布局
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("板块指数K线图仪表板", className="text-center mb-4"),
            html.Hr()
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("指数选择"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("时间范围:"),
                            dcc.Dropdown(
                                id='time-range',
                                options=[
                                    {'label': '最近7天', 'value': 7},
                                    {'label': '最近15天', 'value': 15},
                                    {'label': '最近30天', 'value': 30},
                                    {'label': '最近60天', 'value': 60},
                                    {'label': '最近90天', 'value': 90}
                                ],
                                value=30,
                                clearable=False
                            )
                        ], width=6),
                        dbc.Col([
                            html.Label("显示选项:"),
                            dcc.Checklist(
                                id='display-options',
                                options=[
                                    {'label': '仅显示交易日', 'value': 'trading_days_only'},
                                    {'label': '显示成交量', 'value': 'show_volume'}
                                ],
                                value=['trading_days_only'],
                                inline=True
                            )
                        ], width=6)
                    ]),
                    html.Br(),
                    html.Label("选择指数:"),
                    dcc.Dropdown(
                        id='index-selector',
                        options=index_options,
                        value=index_options[0]['value'] if index_options else None,
                        placeholder="请选择指数..."
                    )
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='candlestick-chart')
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='volume-chart')
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("指数信息"),
                dbc.CardBody([
                    html.Div(id='index-info')
                ])
            ])
        ], width=12)
    ])
], fluid=True)

# 回调函数：更新指数选择器（已移除，现在直接使用统一的指数列表）

# 回调函数：更新K线图
@app.callback(
    Output('candlestick-chart', 'figure'),
    Output('volume-chart', 'figure'),
    Output('index-info', 'children'),
    Input('index-selector', 'value'),
    Input('time-range', 'value'),
    Input('display-options', 'value')
)
def update_charts(selected_index, time_range, display_options):
    if not selected_index:
        empty_fig = go.Figure().add_annotation(
            text="请选择指数",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return empty_fig, empty_fig, "请选择指数"
    
    # 获取数据
    df = get_index_data(selected_index, time_range)
    
    if df.empty:
        empty_fig = go.Figure().add_annotation(
            text="没有数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return empty_fig, empty_fig, "没有找到数据"
    
    # 获取指数名称
    try:
        query = "SELECT index_name FROM index_k_daily WHERE index_code = ? LIMIT 1"
        df_result = db_manager.execute_query(query, (selected_index,))
        index_name = df_result.iloc[0]['index_name'] if not df_result.empty else selected_index
    except:
        index_name = selected_index
    
    # 创建图表
    trading_days_only = 'trading_days_only' in (display_options or [])
    show_volume = 'show_volume' in (display_options or [])
    
    candlestick_fig = create_candlestick_chart(df, selected_index, index_name, trading_days_only, show_volume)
    volume_fig = create_volume_chart(df, selected_index, trading_days_only)
    
    # 创建指数信息
    # 根据显示选项处理数据
    if trading_days_only:
        df_clean = df.dropna(subset=['open', 'high', 'low', 'close'])
        df_clean = df_clean[(df_clean['open'] > 0) & (df_clean['high'] > 0) & 
                           (df_clean['low'] > 0) & (df_clean['close'] > 0)]
    else:
        df_clean = df.copy()
    
    if df_clean.empty:
        info_content = [
            html.H5(f"{index_name} ({selected_index})"),
            html.P("没有有效的交易数据", style={'color': 'red'})
        ]
    else:
        latest_data = df_clean.iloc[-1]
        display_mode = "仅交易日" if trading_days_only else "所有日期"
        info_content = [
            html.H5(f"{index_name} ({selected_index})"),
            html.P(f"显示模式: {display_mode}", style={'color': 'blue', 'font-weight': 'bold'}),
            html.P(f"最新日期: {str(df_clean.index[-1])[:10]}"),
            html.P(f"最新收盘: {latest_data['close']:.2f}"),
            html.P(f"最新成交量: {latest_data['volume']:,}"),
            html.P(f"显示数据点: {len(df_clean)} 个"),
            html.P(f"原始数据点: {len(df)} 个"),
            html.P(f"数据完整性: {len(df_clean)/len(df)*100:.1f}%", style={'color': 'green' if len(df_clean)/len(df) > 0.8 else 'orange'})
        ]
        
        if len(df_clean) > 1:
            change = latest_data['close'] - df_clean.iloc[-2]['close']
            change_pct = (change / df_clean.iloc[-2]['close']) * 100
            change_color = 'red' if change >= 0 else 'green'
            info_content.append(html.P(f"日涨跌: {change:+.2f} ({change_pct:+.2f}%)", style={'color': change_color}))
    
    return candlestick_fig, volume_fig, info_content

if __name__ == '__main__':
    print("启动板块指数K线图仪表板...")
    print("访问地址: http://127.0.0.1:8051")
    app.run(debug=False, host='127.0.0.1', port=8051)
