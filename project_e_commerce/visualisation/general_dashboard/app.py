from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import plotly
import pandas as pd
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

app = Dash(__name__)

main_dir = Path.cwd().parent.parent
list_of_data = {}
for df in (main_dir / "data_intermediate").iterdir():
    if not df.stem == "sample":
        list_of_data[df.stem] = df
def generate_paths_option_dict(list_of_data):
    result_paths_dict = {}
    for data_name, data_dir in list_of_data.items():
        inter_path = data_dir
        paths = {
        "event_type": inter_path / "event_type_distribution.csv",
        "num_distinct": inter_path / "num_of_distinct_user_product_category.csv",
        "num_events": inter_path / "num_events_per_day.csv",
        "num_events_group": inter_path / "num_events_per_day_groupby_type.csv",
        "price": inter_path / "price_distribution.csv"
        }
        result_paths_dict[data_name] = paths
    return result_paths_dict
nec_paths = generate_paths_option_dict(list_of_data)
month_dict = {
    '2019-Oct':'October',
    '2019-Nov':'November',
    }
def_month = '2019-Oct'

def event_type_plot(paths:dict, month:str):
    df = pd.read_csv(paths["event_type"])
    df = df.rename(columns = {
        "count":"Count",
        "event_type": "Type of Event"
        })
    color_map = {
        'view': plotly.colors.qualitative.Bold[5],
        'cart': plotly.colors.qualitative.Bold[4],
        'purchase': plotly.colors.qualitative.Bold[3]
        }
    fig = px.pie(df, 
                values="Count", 
                names="Type of Event",
                color="Type of Event",
                title=f"Types of Events in {month}", 
                color_discrete_map = color_map)
    return fig
def num_events_plot(paths:dict, month:str):
    df = pd.read_csv(paths["num_events"])
    df = df.rename(columns ={"event_date": "Date",
            "event_type": "Number of Events"})
    fig = px.bar(df, 
                x="Date", 
                y="Number of Events",
                title=f"Number of Events during {month}",
                color_discrete_sequence=plotly.colors.qualitative.Bold[8:9],)
    return fig
def num_events_group_plot(paths:dict, month:str):
    df = pd.read_csv(paths["num_events_group"])
    df = df.rename(columns ={"event_date": "Date",
                "event_type": "Type of Events",
                "event_time": "Number of Events"})
    color_map = {
        'view': plotly.colors.qualitative.Bold[5],
        'cart': plotly.colors.qualitative.Bold[4],
        'purchase': plotly.colors.qualitative.Bold[3]
        }
    fig = px.bar(df, 
                    x="Date", 
                    y="Number of Events",
                    color="Type of Events",
                    title=f"Number of Events during {month}, by Type of Events",
                    barmode="group",
                color_discrete_map=color_map,)
    return fig
def price_distr_plot(paths:dict, month:str):
    df = pd.read_csv(paths["price"])
    fig = go.Figure()

    ht = [
        f"Price Range: ({df['Price'][i-1] if i > 0 else 0}, {df['Price'][i]}), Density: {density:.4f}"
        for i, density in enumerate(df['Density'])
    ]
    # Add bar trace
    fig.add_trace(go.Bar(
        x=df['Price'],
        y=df['Density'],
        marker=dict(color=plotly.colors.qualitative.Bold[8]),
        hoverinfo="text",
        hovertext= ht
    ))

    # Update layout to remove gaps between bars
    fig.update_layout(
        title=f'Density Histogram of Price Levels in {month}',
        xaxis_title='Price Levels',
        yaxis_title='Density',
        bargap=0,
    )
    return fig
def distinct_user_product_category(paths:dict):
    df = pd.read_csv(paths["num_distinct"], index_col=0).reset_index().transpose()[0]
    df.pop("index")
    return df.to_dict()
def single_num_plot(title, display_num):
    fig = go.Figure(go.Indicator(
        mode="number+delta",
        value=display_num,
        title = {"text": title},
        number={'valueformat': ','}
    ))
    # fig.update_layout(
    #     title={
    #         'text':title,
    #         'xanchor': 'center',
    #         'yanchor':'top'}
    # )
    return fig

app.layout = html.Div(
    [html.H1(style={'textAlign':'center'}, id='title'),
     html.Div([
         dcc.Dropdown(list(nec_paths.keys()), def_month, id='data_selection_dropdown')
     ]),
     html.Div([
         html.Div(dcc.Graph(id='event_type_plot', figure=event_type_plot(nec_paths[def_month], month_dict[def_month])), className="graph_dv"),
         html.Div(dcc.Graph(id='num_events_plot', figure=num_events_plot(nec_paths[def_month], month_dict[def_month])), className="graph_dv"),
         html.Div(dcc.Graph(id='num_events_group_plot', figure=num_events_group_plot(nec_paths[def_month], month_dict[def_month])), className="graph_dv"),
         html.Div(dcc.Graph(id='price_plot', figure=price_distr_plot(nec_paths[def_month], month_dict[def_month])), className="graph_dv"),
         html.Div(dcc.Graph(id='user_disp', figure=single_num_plot(f'Number of Distinct Users in {month_dict[def_month]}', distinct_user_product_category(nec_paths[def_month])['Number of Distinct Users'])), className="graph_dv"),
         html.Div(dcc.Graph(id='cat_disp', figure=single_num_plot(f'Number of Distinct Product Categories in {month_dict[def_month]}', distinct_user_product_category(nec_paths[def_month])['Number of Distinct Categories'])), className="graph_dv"),
         html.Div(dcc.Graph(id='prod_disp', figure=single_num_plot(f'Number of Distinct Products in {month_dict[def_month]}', distinct_user_product_category(nec_paths[def_month])['Number of Distinct Products'])), className="graph_dv"),
     ],id="graphs_div", style={
            'display': 'grid',
            'gridTemplateColumns': 'repeat(2, 1fr)',
            'gap': '10px'  # Adjust the gap between grid items as needed
        })
        
     ]
     
)

@app.callback(
        Output('title', 'children'),
        Input('data_selection_dropdown', 'value'))
def update_title(selected):
    month = month = month_dict.get(selected, 'Unknown Month')
    return f"General Analysis of {month}"

@app.callback(
    Output('event_type_plot', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_event_type_plot(selected):
    return event_type_plot(nec_paths[selected], month_dict[selected])

@app.callback(
    Output('num_events_plot', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_num_events_plot(selected):
    return num_events_plot(nec_paths[selected], month_dict[selected])

@app.callback(
    Output('num_events_group_plot', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_num_events_group_plot(selected):
    return num_events_group_plot(nec_paths[selected], month_dict[selected])

@app.callback(
    Output('price_plot', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_price_plot(selected):
    return price_distr_plot(nec_paths[selected], month_dict[selected])

@app.callback(
    Output('user_disp', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_user(selected):
    return single_num_plot(f'Number of Distinct Users in {month_dict[selected]}', distinct_user_product_category(nec_paths[selected])['Number of Distinct Users'])

@app.callback(
    Output('cat_disp', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_cat(selected):
    return single_num_plot(f'Number of Distinct Product Categories in {month_dict[selected]}', distinct_user_product_category(nec_paths[selected])['Number of Distinct Categories'])

@app.callback(
    Output('prod_disp', 'figure'),
    Input('data_selection_dropdown', 'value'))
def update_prod(selected):
    return single_num_plot(f'Number of Distinct Products in {month_dict[selected]}', distinct_user_product_category(nec_paths[selected])['Number of Distinct Products'])

if __name__ == '__main__':
    app.run(debug=True)