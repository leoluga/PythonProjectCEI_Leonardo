from dash import Dash, html, dcc, callback, Output, Input, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import pandas as pd
import copy


class DashboardComponents:

    @staticmethod
    def make_card_from_series(df: pd.DataFrame, field_column) -> html.Div:
        assert field_column in df.columns
        fig = DashboardComponents.plot_one_time_series(df.index, df[field_column].values)
        card_body = DashboardComponents.make_plotly_card(fig, f"{field_column}-graph")

        card_with_title = html.Div([
            html.Div(field_column, className="main-card-label"),
            card_body
        ], className="main-card")

        return card_with_title
    
    @staticmethod
    def make_plotly_card(fig: go.Figure, id_card: str, class_name_str = "card-body") -> html.Div:
        dcc_graph = dcc.Graph(id = id_card, figure = fig)
        return html.Div([dcc_graph], className=class_name_str)
        
    @staticmethod
    def plot_one_time_series(x_values: pd.Series, y_values: pd.Series) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=x_values, y=y_values,
                        mode='lines+markers',
                        name='lines'))
        fig.update_layout(
            xaxis=dict(
                showline=True,
                showgrid=True,
                showticklabels=True,
                gridwidth = 0.5,
                gridcolor = 'rgba(255, 255, 255, 0.1)',
                linecolor='rgb(204, 204, 204, 0.5)',
                linewidth=2,
                ticks='outside',
                tickfont=dict(
                    family='Arial',
                    size=12,
                    color='white',
                ),
            ),
            yaxis=dict(
                showgrid=True,
                gridwidth = 0.5,
                gridcolor = 'rgba(255, 255, 255, 0.1)',
                zeroline=False,
                showline=False,
                showticklabels=True,
                tickfont=dict(
                    family='Arial',
                    size=12,
                    color='white',
                ),
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=35,
                r=10,
                t=10,
                pad = 4
            ),
            showlegend=True,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor = 'rgba(0,0,0,0)'
        )

        return fig