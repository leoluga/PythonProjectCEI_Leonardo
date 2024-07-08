from dash import Dash, html, dcc, callback, Output, Input, State
from dash.exceptions import PreventUpdate
import plotly.express as px
import pandas as pd
import copy
from CatalogDataReader import CatalogDataReader
from DataConverter import DataConverter
from TelemetryDataReader import TelemetryDataReader

MAIN_SPORT_DOCUMENT_FILE_PATH = 'SPORT_documents//sport_ttc_20220814.ods'

telemetry_reader = TelemetryDataReader()
catalog_data = CatalogDataReader()
data_converter = DataConverter()

main_tm_df = catalog_data.get_all_tms_on_the_document(MAIN_SPORT_DOCUMENT_FILE_PATH)
main_dd_df = catalog_data.get_all_dds_from_document(MAIN_SPORT_DOCUMENT_FILE_PATH)

telemetry_file_path = 'decoded_satcs_dump\\hk_vur_inst_only.out'
df = telemetry_reader.get_space_packets_df_from_file(telemetry_file_path)

formats_to_implement = []
def calculate_data_conversion(apid:str, binary_data: int, main_dd_df: pd.DataFrame):

    new_data_packets = copy.deepcopy(main_dd_df.query(f"""apid == '{apid}'""")['data_packets'].item())
    pointer = 0
    for single_data_field in new_data_packets:
        bit_length = single_data_field['lenght(bits)']
        if (bit_length is None) or (bit_length == 'N/A'):
            bit_length = 0
        elif bit_length >= len(binary_data) or single_data_field['field'] == 'Total':
            break
        else:
            bit_length = int(bit_length)
    
        binary_slice = binary_data[pointer:(pointer+bit_length)]
        
        assert len(binary_slice) == bit_length, f"{single_data_field}"
        
        data_format = single_data_field['format']
        try:
            transformed_value = data_converter.binary_to_value(binary_slice, data_format, single_data_field['conversion'])
        except ValueError:
            print(f"Implement this format: {data_format} - {binary_slice}")
            if data_format not in formats_to_implement:
                formats_to_implement.append(data_format)
            transformed_value = None

        single_data_field['value'] = transformed_value
        pointer += bit_length

    return new_data_packets

new_fields = []
for i in range(0, len(df)):
    apid = df.iloc[i,:]['apid']
    binary_data = df.iloc[i,:]['data']
    
    new_fields.append(calculate_data_conversion(apid, binary_data, main_dd_df))

assert len(df) == len(new_fields)

df['data_transformed'] = new_fields
available_apids = df['apid'].unique()

apid_df_dict = dict()
for apid in df['apid'].unique():
    apid_linked_dd = main_dd_df.query(f"""apid == '{apid}'""")['data_packets'].item()
    inner_df = df.query(f"""apid == '{apid}'""").copy(deep=True)
    
    new_df = inner_df['secondary_header'].reset_index().copy(deep=True)
    for field_id in range(0, len(apid_linked_dd)):
        y_aux = []
        for i in range(0, len(inner_df)):
            field_data = (inner_df.iloc[i, :]['data_transformed'])[field_id]

            field_name = field_data['field']
            field_unit = field_data['unit']

            if 'value' in field_data.keys():
                y_aux.append(field_data['value'])
        if field_name != 'Total':
            new_df[f"{field_name} ({field_unit}) {field_id}"] = y_aux

    new_df = new_df.rename(columns={'secondary_header':'time'})
    new_df = new_df.drop(columns='index').set_index('time')

    apid_df_dict[apid] = new_df


app = Dash(
    __name__,
    assets_folder='assets'
)

app.layout = html.Div([
    html.H1(
        className='header-title',
        children='Sports Mission Dashboard',
        style={'textAlign':'center','color': '#FFFFFF'}
             
    ),
    dcc.Dropdown(available_apids, id='apid-selection'),
    dcc.Dropdown(['n/a'], id='fields-selection', searchable=True),
    dcc.Graph(id='graph-content')
])

@callback(
    Output('fields-selection', 'options'),
    Input('apid-selection', 'value'),
    prevent_initial_call=True
)
def update_fields(apid_value):
    if apid_value is None:
        raise PreventUpdate
    apid_row = main_dd_df.query(f"""apid == '{apid_value}'""")

    possible_fields = []
    for i, dict in enumerate(apid_row['data_packets'].item()):
        field = dict['field']
        unit = dict['unit']
        if field == 'Total':
            break
        possible_fields.append(f"{field} ({unit}) {i}")

    return possible_fields

@callback(
    Output('graph-content', 'figure'),
    Input('fields-selection', 'value'),
    State('apid-selection', 'value'),
    prevent_initial_call=True
)
def update_graph(field, apid):
    if field is None or apid is None:
        raise PreventUpdate
    plot_df = apid_df_dict[apid].loc['2020-01-01':].dropna(axis=1)

    return px.line(x =plot_df.index, y=plot_df[field])

if __name__ == '__main__':
    app.run(debug=True)
