from dash import Dash, html, dcc, callback, Output, Input, State
from dash.exceptions import PreventUpdate
import pandas as pd

from CatalogDataReader import CatalogDataReader
from DataConverter import DataConverter
from TelemetryDataReader import TelemetryDataReader
from FileRepository import FileRepository
from DashboardComponents import DashboardComponents as mission_dash_components

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
                searchable=True,
            ),
        ], className="single-input-div"),
        html.Div([
            html.Div("Select APID", className="single-input-label"),
            dcc.Dropdown(
                id='apid-selection', 
                searchable=True,
            ),
        ], className="single-input-div"),
    ], 
    className="upper-dashboard-inputs"
)

lower_inputs_layout = html.Div([
        html.Div([
            html.Div("Select Fields to Display", className="single-input-label"),
            dcc.Dropdown(
                id='fields-selection', 
                searchable=True, 
                multi=True,
                maxHeight=300 
            ),
        ], className="single-input-div"),
    ], 
    className="lower-dashboard-inputs"
)

inputs_layout = html.Div([
    upper_inputs_layout,
    lower_inputs_layout
], className="dashboard-inputs")

main_layout = html.Div([
    html.Div(className="background-overlay"),
    header_layout,
    dcc.Loading(
            [inputs_layout],
            overlay_style={"visibility":"visible", "opacity": .1, "backgroundColor": "grey"},
            className='gif-loading'
        ),
    html.Div(id="main-dashboard-content", className="main-content-div")
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
    print("updating telemetry data")
    return telemetry_data_dict

@callback(
    Output('apid-selection', 'options'),
    Output('space-packets-data', 'data'),
    Output('fields-selection', 'value'),
    Input('dump-file-selection', 'value'),
    State('main-telemetry-data', 'data'),
)
def update_apids_available_and_space_packets_df(dump_file_selected, telemetry_data_dict):
    if dump_file_selected is None or telemetry_data_dict is None:
        raise PreventUpdate
    main_dd_df = pd.read_json(telemetry_data_dict["main_dd_df"], orient = 'split')
    
    space_packets_df = telemetry_reader.get_space_packets_df_from_file(dump_file_selected, main_dd_df)
    available_apids = space_packets_df['apid'].unique()
    
    space_packets_dict = {
        "space_packets_df": space_packets_df.to_json(orient='split', date_format='iso')
    }
    print("updating available_apids and space_packets_dict")
    return available_apids, space_packets_dict, None

@callback(
    Output('fields-selection', 'options'),
    Output('fields-apid-data', 'data'),
    Input('apid-selection', 'value'),
    State('space-packets-data', 'data'),   
    State('main-telemetry-data', 'data'),
    prevent_initial_call=True
)
def update_fields(apid_value, space_packets_dict ,telemetry_data_dict):
    if (apid_value is None) or (space_packets_dict is None) or (telemetry_data_dict is None):
        raise PreventUpdate
    
    main_dd_df = pd.read_json(telemetry_data_dict["main_dd_df"], orient = 'split')
    space_packets_df = pd.read_json(space_packets_dict["space_packets_df"], orient = 'split')

    fields_apid_df = telemetry_reader.get_specific_apid_df_from_telemetry_df(apid_value, space_packets_df, main_dd_df)
    fields_available = [k for k in list(fields_apid_df.columns) if k != 'secondary_header']

    fields_apid_dict = {
        "fields_apid_df": fields_apid_df.reset_index().to_json(orient='split', date_format='iso')
    }

    return fields_available, fields_apid_dict

@callback(
    Output('main-dashboard-content', 'children'),
    Input('fields-selection', 'value'),
    State('apid-selection', 'value'),
    State('fields-apid-data', 'data'),
    prevent_initial_call=True
)
def update_graph(fields, apid, fields_apid_dict):
    if (fields is None) or (apid is None) or (len(fields) == 0) or (fields_apid_dict is None):
        return html.Div([])
    
    fields_apid_df = pd.read_json(fields_apid_dict["fields_apid_df"], orient = 'split')
    if 'secondary_header' in fields_apid_df.columns:
        fields_apid_df = fields_apid_df.set_index('secondary_header')
        fields_apid_df = fields_apid_df.loc['2020-01-01':].dropna(axis=1)
    
    field_cards = []
    for field in fields:
        field_card = mission_dash_components.make_card_from_series(fields_apid_df, field)
        field_cards.append(field_card)
    
    return field_cards
   
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
