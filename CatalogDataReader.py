import pandas as pd
import numpy as np
import ezodf

from SpacePacketDefinitions import SpacePacketDefinitions

class CatalogDataReader:
    """This class is for reading all the catalog data from a main file.
    This will be the data for TC, TM, DD, DC.
    """
    space_packets: SpacePacketDefinitions
    
    def __init__(self) -> None:
        self.space_packets = SpacePacketDefinitions()

    def get_all_tms_on_the_document(self, file_path: str) -> pd.DataFrame:
        """Specific to read all TM sheets in the document."""
        tm_sheets = self.get_specific_sheet_names_from_document(file_path, "TM")
        columns = [
            'identification',
            'name',
            'version_number', 
            'pkt_type', 
            'sec_hdr_flag', 
            'apid', 
            'seq_flags', 
            'seq_count', 
            'pkt_data_length', 
            'secondary_header', 
            'data', 
            'checksum'
        ]

        main_tm_df = pd.DataFrame()
        for sheet_name in tm_sheets:
            df = self.read_document_by_sheet(file_path, sheet_name)
            df = df.iloc[6:, 0:12].dropna(axis = 0, how = 'all')

            main_tm_df = pd.concat([main_tm_df, df], ignore_index=True)

        main_tm_df.columns = columns
        main_tm_df['apid'] = main_tm_df['apid'].apply(lambda x: hex(int(x, 16)))

        return main_tm_df
    
    def get_all_dds_from_document(self, file_path: str):
        """Specific to read all DD sheets data in the document."""
        main_tm_df = self.get_all_tms_on_the_document(file_path)

        dd_sheets = self.get_specific_sheet_names_from_document(file_path, "DD")

        all_dd_from_sheets_df = pd.DataFrame()
        for sheet_name in dd_sheets:
            inner_dd_df = self.read_dd_sheet_from_document(file_path, sheet_name, main_tm_df)
            all_dd_from_sheets_df = pd.concat([all_dd_from_sheets_df, inner_dd_df], ignore_index=True)

        return all_dd_from_sheets_df

    def read_dd_sheet_from_document(self, file_path: str, sheet_name: str, main_tm_df: pd.DataFrame):

        df = self.read_document_by_sheet(file_path, sheet_name)
        df = df.dropna(axis = 0, how = 'all').dropna(axis = 1, how = 'all')
        data_array = df.to_numpy()

        pointer = 0
        all_sheet_data = []
        for i in range(0, len(data_array)):
            first_column_value = data_array[i,0]
            if first_column_value == 'Total':
                inner_data_array = data_array[pointer:(i+1), :]
                all_sheet_data.append(self.read_inner_dd_from_document_array(inner_data_array, main_tm_df))
                pointer = (i+1)

        return pd.DataFrame(all_sheet_data)

    def read_inner_dd_from_document_array(self, data_array: np.ndarray, main_tm_df: pd.DataFrame):
        assert data_array[0,0] is not None
        assert data_array[1,0] == 'Field'
        assert data_array[-1,0] == 'Total'
        
        data_name = data_array[0,0]
        identification, apid = self.find_tm_for_given_dd_name(data_name, main_tm_df)

        main_dict = {
            "identification": identification,
            "apid": apid,
            "data_name": data_array[0,0]
        }
        
        data_packets = []
        for i in range(2, len(data_array)):
            row = data_array[i,:]
            inner_packet = self.space_packets.single_data_field_dict_from_row(row)
            data_packets.append(inner_packet)
        main_dict["data_packets"] = data_packets

        return main_dict

    def find_tm_for_given_dd_name(self, dd_name: str, main_tm_df: pd.DataFrame) -> tuple[str, str]:
        filtered_tm_df = main_tm_df.drop_duplicates().query(f"""data == '{dd_name}'""")

        if filtered_tm_df.empty:
            return '', ''
        else:
            return filtered_tm_df['identification'].item(), filtered_tm_df['apid'].item()

    def read_document_by_sheet(self, file_path, sheet_name):
        """reads specific sheet from document."""
        ods = ezodf.opendoc(file_path)
        sheet = ods.sheets[sheet_name]
        
        data = []
        for row in sheet.rows():
            data_row = [cell.value for cell in row]
            data.append(data_row)
        
        df = pd.DataFrame(data)
        return df
    
    def get_specific_sheet_names_from_document(self, file_path: str, starts_with: str) -> list:
        """Filter the sheets based on the first characters."""
        ods = ezodf.opendoc(file_path)
        sheets = [name for name in ods.sheets.names() if name.startswith(starts_with)]

        return sheets

