# stdlib
import time
import sys

# spclib
import psycopg2
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output



def get_data_for_param(conn, parameter_id, dataset_id=None, region_id=None, show_parameter="moving_avg", dataset_names=False):# ["count", "sum", "moving_count_sum", "moving_total_sum", "moving_avg"]

    #selector = ' AND '.join([str(param) + '=' + str(value) for param, value in colsfromPost.items()])
    select = ''
    if dataset_id != None:
        select += f' AND dataset_id={dataset_id}'
    if region_id != None:
        select += f' AND region_id={region_id}'

    # moving_avg_query_wc = """WITH prep_tbl AS
    #   (SELECT date, COUNT(*) as count, SUM(value) as sum
    #    FROM yuh_general_datapoint WHERE parameter_id={parameter_id} {select}
    #    GROUP BY 1 ORDER BY date),

    #   total_tbl AS
    #   (SELECT date,  count, sum,
    #     SUM(count) OVER (ORDER BY date RANGE BETWEEN '30 days' PRECEDING AND current row) AS moving_count_sum,
    #     SUM(sum) OVER (ORDER BY date RANGE BETWEEN '30 days' PRECEDING AND current row) AS moving_total_sum FROM prep_tbl)

    #   SELECT date, count, sum, moving_count_sum, moving_total_sum,moving_total_sum/moving_count_sum AS moving_avg FROM total_tbl;"""


    moving_avg_query_wc = """WITH prep_tbl AS (SELECT
    date, COUNT(*) as count, SUM(value) AS sum, stddev(value) AS stdev, dataset_id
    FROM yuh_general_datapoint WHERE parameter_id={parameter_id} {select}
    GROUP BY date, dataset_id ORDER BY date),

    total_tbl AS
    (SELECT date,  count, sum, dataset_id, stdev,
    SUM(count) OVER (PARTITION BY dataset_id ORDER BY date RANGE BETWEEN '30 days' PRECEDING AND current row) AS moving_count_sum, 
    SUM(sum) OVER (PARTITION BY dataset_id ORDER BY date RANGE BETWEEN '30 days' PRECEDING AND current row) AS moving_total_sum FROM prep_tbl) 

    SELECT date, count, sum, stdev, moving_count_sum, moving_total_sum,moving_total_sum/moving_count_sum AS moving_avg,dataset_id FROM total_tbl LEFT JOIN yuh_general_dataset on dataset_id=yuh_general_dataset.id;"""

    

    query = moving_avg_query_wc.format(parameter_id=parameter_id, select=select)
    data = pd.read_sql_query (query, conn)

    edges = pd.date_range(data[['date']].iloc[0].date, data[['date']].iloc[-1].date, freq='D')
    data = data.pivot('date', 'dataset_id', show_parameter)
    #print(data[data.index.duplicated()])
    data = data.reindex(edges, fill_value=pd.NA)
    dataset_names = dict((v+1,k) for k,v in dataset_names.items())
    data.columns = [dataset_names[col] for col in data.columns]
    
    return data

def connectDB():

    conn = psycopg2.connect(
        host="localhost",
        database="yuh4",
        user="django",
        password="cafebabe")

    cur = conn.cursor()
    return conn, cur

def getMedParams(conn):
    query = """SELECT * from yuh_general_parameter;"""
    params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
    params = dict((v,k) for k,v in params.items())
    return params


def getRegionParams(conn):
    query = """SELECT * from yuh_general_region;"""
    params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
    params = dict((v,k) for k,v in params.items())
    return params


def getDatasetParams(conn):
    query = """SELECT * FROM yuh_general_dataset;"""
    params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
    params = dict((v,k) for k,v in params.items())
    return params

def getDatasetNames(conn):
    query = """SELECT * FROM yuh_general_dataset;"""
    params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
    params = dict((v,k) for k,v in params.items())
    return params


# def getSexParams(conn):
#     query = """SELECT * from yuh_general_region;"""
#     params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
#     params = dict((v,k) for k,v in params.items())
#     return params


# def getAgeGroupParams(conn):
#     query = """SELECT * from yuh_general_region;"""
#     params = pd.read_sql_query(query, conn)['name_ru'].to_dict()
#     params = dict((v,k) for k,v in params.items())
#     return params


def getApp(conn, parameters, dataset_id_options, region_options):
    app = Dash(__name__)

    app.layout = html.Div([
        html.H4('Some words'),
        dcc.Loading(
                id="loading",
                children=[
                    dcc.Graph(id='time-series-chart'),
                    html.Div(id="loading-output"),
                ]
        ),
        html.P("Select:"),
        dcc.Dropdown(
            id="ticker",
            options=list(parameters.keys()),
            value=list(parameters.keys())[0],
            clearable=False,
        ),
        dcc.Dropdown(
            id="ticker1",
            options=["count", "sum", "moving_count_sum", "moving_total_sum", "moving_avg"],
            value="moving_avg",
            clearable=False,
        ),
        # dcc.Dropdown(
        #     id="dataset_id",
        #     options=list(dataset_id_options.keys()),
        #     #value=dataset_id_options[0],
        #     value=None,
        #     clearable=True,
        # ),
        dcc.Dropdown(
            id="region_name",
            options=list(region_options.keys()),
            #value=list(region_options.keys())[0],
            value=None,
            clearable=True,
        ),
        ])

    @app.callback(Output("loading", "loading-output"), Input("time-series-chart", 'loading_state'))
    def loader(value):
        time.sleep(1)
        return value

    @app.callback(
        Output("time-series-chart", "figure"), 
        Input("ticker", "value"),
        Input("ticker1", "value"),
        # Input("dataset_id", "value"),
        Input("region_name", "value"))
    
    def display_time_series(ticker, ticker1, region_name):

        fig = px.line(
            get_data_for_param(
                conn, 
                parameters[ticker], 
                # dataset_id_options[dataset_id] if dataset_id != None else dataset_id, 
                region_options[region_name] if region_name != None else region_name,
                show_parameter=ticker1,
                dataset_names=dataset_id_options
            ), 
            #y=ticker1
            # color=dataset_id
        )
        
        return fig

    return app


def main(host, port):
    conn, cur = connectDB()
    params = getMedParams(conn)
    datasets = getDatasetParams(conn)
    regions = getRegionParams(conn)
    app = getApp(conn, params, datasets, regions)
    try:
        app.run_server(debug=True, host=host, port=port)
    except Exception as ex:
        print(ex)
    finally:
        conn.close()
        app.close()



if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
