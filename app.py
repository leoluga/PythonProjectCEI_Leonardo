from dash import Dash, html, dcc, callback, Output, Input, State, ALL
from dash.exceptions import PreventUpdate

import pandas as pd
import argparse

from space_packets_pkg.CatalogDataReader import CatalogDataReader
from space_packets_pkg.DataConverter import DataConverter
from space_packets_pkg.TelemetryDataReader import TelemetryDataReader
from space_packets_pkg.FileRepository import FileRepository

from app_components_pkg.DashboardComponents import DashboardComponents as mission_dash_components

CATALOG_FOLDER = "SPORT_documents"
TELEMETRY_DUMP_FOLDER = "decoded_satcs_dump"

catalog_repo = FileRepository(CATALOG_FOLDER)
telemetry_repo = FileRepository(TELEMETRY_DUMP_FOLDER)

MAIN_SPORT_DOCUMENT_FILE_NAME = 'sport_ttc_20220814.ods'

telemetry_reader = TelemetryDataReader()
catalog_data = CatalogDataReader()
data_converter = DataConverter()

app = Dash(
    __name__,
    assets_folder='assets',
    title = "CEI Mission Dashboard"
)

header_layout = html.Div(
    className='header-container',
    children=[
        html.Img(
            src='/assets/cei_logo.png', 
            className='dashboard-header-logo',
        ),
        html.H1(
            className='dashboard-header-title',
            children='Mission Data Dashboard',
            style={'textAlign': 'center', 'color': '#FFFFFF', 'flexGrow': '1'}
        ),
        html.Div(className="dummy-header")
    ],
)

upper_inputs_layout = html.Div([
        dcc.Store(id = "main-telemetry-data"),
        dcc.Store(id = "space-packets-data"),
        dcc.Store(id = "fields-apid-data"),
        dcc.Store(id = "fields-apid-data-teste"),
        html.Div([
            html.Div("Catalog File", className="single-input-label"),
            dcc.Dropdown(
                id='catalog-selection', 
                value = MAIN_SPORT_DOCUMENT_FILE_NAME,
                options = [k for k in catalog_repo.list_files() if '.pdf' not in k],
                searchable=True, 
                disabled=True
            ),
        ], className="single-input-div"),
        html.Div([
            html.Div("Select Telemetry File", className="single-input-label"),
            dcc.Dropdown(
                id='dump-file-selection', 
                options = telemetry_repo.list_files(),
            ),
        ], className="single-input-div"),
        html.Div([
            html.Div("Select APID", className="single-input-label"),
            dcc.Dropdown(
                id='apid-selection', 
                multi=True,
            ),
        ], className="single-input-div"),
    ], 
    className="upper-dashboard-inputs"
)

lower_inputs_layout = html.Div(
    id="fields-inputs",
    className="lower-dashboard-inputs"
)

inputs_layout = html.Div([
    header_layout,
    upper_inputs_layout,
    lower_inputs_layout
], className="dashboard-inputs")

main_layout = html.Div([
    html.Div(className="background-overlay"),
    html.Div([
        dcc.Loading(
                [inputs_layout],
                overlay_style={"visibility":"visible", "opacity": .1, "backgroundColor": "grey"},
                className='gif-loading'
            ),
        html.Div(id="main-dashboard-plots", className="main-content-div-plots"),
        html.Div(id="main-dashboard-tables", className="main-content-div-tables"),
    ], className = "inputs-and-content-div"),
], className="dashboard-div")

app.layout = main_layout

@callback(
    Output('main-telemetry-data', 'data'),
    Input('catalog-selection', 'value'),
)
def update_stored_data(catalog_file_name):
    if catalog_file_name is None:
        raise PreventUpdate

    main_tm_df = catalog_data.get_all_tms_on_the_document(catalog_file_name)
    main_dd_df = catalog_data.get_all_dds_from_document(catalog_file_name)

    telemetry_data_dict = {
        "main_tm_df": main_tm_df.to_json(orient='split', date_format='iso'),
        "main_dd_df": main_dd_df.to_json(orient='split', date_format='iso')
    }
    print("Updating telemetry data")
    return telemetry_data_dict

@callback(
    Output('apid-selection', 'options'),
    Output('space-packets-data', 'data'),
    Input('dump-file-selection', 'value'),
    State('main-telemetry-data', 'data'),
)
def update_apids_available_and_space_packets_df(dump_file_selected, telemetry_data_dict):
    if dump_file_selected is None or telemetry_data_dict is None:
        raise PreventUpdate
    main_dd_df = pd.read_json(telemetry_data_dict["main_dd_df"], orient = 'split')
    
    space_packets_df = telemetry_reader.get_space_packets_df_from_file(dump_file_selected, main_dd_df)
    available_apids = space_packets_df['apid'].unique()
    available_apids_data_names = telemetry_reader.query_main_dd_df_for_apid_data_name(available_apids, main_dd_df)
    apid_options = [{'label': data_name, 'value': apid} for apid, data_name in zip(available_apids, available_apids_data_names)]
    
    space_packets_dict = {
        "space_packets_df": space_packets_df.to_json(orient='split', date_format='iso')
    }
    print("Updating available_apids and space_packets_dict")
    return apid_options, space_packets_dict

@callback(
    Output('fields-inputs', 'children'),
    Output('fields-apid-data', 'data'),
    Input('apid-selection', 'value'),
    State('space-packets-data', 'data'),   
    State('main-telemetry-data', 'data'),
    prevent_initial_call=True
)
def update_fields_teste(apid_list, space_packets_dict ,telemetry_data_dict):
    if (apid_list is None) or (space_packets_dict is None) or (telemetry_data_dict is None):
        raise PreventUpdate

    main_dd_df = pd.read_json(telemetry_data_dict["main_dd_df"], orient = 'split')
    space_packets_df = pd.read_json(space_packets_dict["space_packets_df"], orient = 'split')
    
    fields_apid_dict = {}
    fields_inputs_children = []
    for i, apid in enumerate(apid_list):
        fields_apid_df = telemetry_reader.get_specific_apid_df_from_telemetry_df(apid, space_packets_df, main_dd_df)
        fields_available = [k for k in list(fields_apid_df.columns) if k != 'secondary_header']

        fields_apid_dict[f"{apid}_fields_df"] = fields_apid_df.reset_index().to_json(orient='split', date_format='iso')
        apid_name = telemetry_reader.query_main_dd_df_for_apid_data_name(apid, main_dd_df)
        
        inner_children = html.Div([
            html.Div(f"Select {apid_name} Fields to Display", className="single-input-label"),
            dcc.Dropdown(
                id={"type": "fields-selection-teste", "index":i }, 
                options = fields_available,
                searchable=True, 
                multi=True,
                maxHeight=400,
                persistence=True
            ),
        ], className="single-input-div")
        fields_inputs_children.append(inner_children)

    return fields_inputs_children, fields_apid_dict

@callback(
    Output('main-dashboard-plots', 'children'),
    Output('main-dashboard-tables', 'children'),
    Input({'type':'fields-selection-teste', "index": ALL}, "value"),
    State('apid-selection', 'value'),
    State('fields-apid-data', 'data'),
    prevent_initial_call=True
)
def update_graphs_teste(fields, apid_list, fields_apid_dict):
    if (fields is None) or (apid_list is None) or (len(fields) == 0) or (fields_apid_dict is None):
        return html.Div([]), html.Div([])

    field_cards = []
    fields_selected_df = pd.DataFrame()
    for i, apid in enumerate(apid_list):
        
        field_list_for_apid = fields[i]
            
        if field_list_for_apid is None:
            pass
        else:
            fields_apid_df = pd.read_json(fields_apid_dict[f"{apid}_fields_df"], orient = 'split')
            fields_apid_df = fields_apid_df[field_list_for_apid]
            if 'secondary_header' in fields_apid_df.columns:
                fields_apid_df = fields_apid_df.set_index('secondary_header')
                fields_apid_df = fields_apid_df.loc['2020-01-01':].dropna(axis=1) #This will not be needed in future, it is due to a reset problem in the Sports Satellite.
            
            fields_selected_df = pd.concat([fields_selected_df,fields_apid_df], axis=1)

            for field in field_list_for_apid:
                field_card = mission_dash_components.make_card_from_series(fields_apid_df, field)
                field_cards.append(field_card)
    
    history_of_apids_card = mission_dash_components.ag_grid_inputs_from_historical_df(fields_selected_df)
    
    last_fields_values_df = fields_selected_df.apply(lambda col: col.loc[col.last_valid_index()])
    last_fields_values_df = last_fields_values_df.reset_index()
    last_fields_values_df.columns = ['Fields','Last Value']
    last_fields_ag_grid_card = mission_dash_components.ag_grid_inputs_from_last_values_df(last_fields_values_df)
    tables_div = html.Div([last_fields_ag_grid_card,history_of_apids_card], className="main-content-div-tables-inner")
    return field_cards, tables_div

def main():
    parser = argparse.ArgumentParser(description='Run the Dash app.')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host address')
    parser.add_argument('--port', type=int, default=8050, help='Port number')
    parser.add_argument('--debug', action='store_true', help='Run the app in debug mode')
    args = parser.parse_args()

    app.run_server(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()