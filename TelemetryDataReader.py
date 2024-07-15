
import copy
import pandas as pd

from DataConverter import DataConverter
from SpacePacketDefinitions import SpacePacketDefinitions
from FileRepository import FileRepository

TELEMETRY_FOLDER_PATH = "decoded_satcs_dump"
class TelemetryDataReader:
    file_repo: FileRepository
    data_converter: DataConverter 
    space_packets: SpacePacketDefinitions

    def __init__(self) -> None:
        self.file_repo = FileRepository(TELEMETRY_FOLDER_PATH)
        self.data_converter = DataConverter()
        self.space_packets = SpacePacketDefinitions()

    def get_space_packets_df_from_file(self, file_name: str, transform_binary_values: bool = True) -> pd.DataFrame:
        """Easier way to get the df directly from the file_path."""
        space_packets = self.read_file_and_get_space_packets(file_name)
        df = self.create_df_from_space_packets(space_packets, transform_binary_values)
        return df
    
    def read_file_and_get_space_packets(self, file_name: str) -> list[dict]:
        """Attempts to read a normal hex file, if error it will read as if it were a binary file.
        After being able to read it will parse the hex string into space packets format."""
        hex_data = self.file_repo.read_telemetry_dump_file(file_name)
        packets = self.read_through_hex_str(hex_data)
        return packets

    def create_df_from_space_packets(self, packets: list[dict], main_dd_df: pd.DataFrame = None,transform_binary_values: bool = True) -> pd.DataFrame:
        """Allows the space packets to be displayes as df format, also it performs transformations to the binary values of
        a space packet. When asked to transform for binary values the function will also adjust for segmented packets."""
        df = pd.DataFrame(packets)
        if transform_binary_values:
            assert main_dd_df is not None, "For transformation main_dd_df must be inputed!"
            df['version_number'] = df['version_number'].apply(lambda x: int(x, 2))
            df['apid'] = df['apid'].apply(lambda x: hex(int(x, 2)))
            df['seq_flags'] = df['seq_flags'].apply(lambda x: hex(int(x, 2)))
            df['pkt_data_length'] = df['pkt_data_length'].apply(lambda x: hex(int(x, 2)))
            df['secondary_header'] = df['secondary_header'].apply(self.data_converter.convert_64bit_binary_to_datetime)
            df = self.adjust_df_for_segmented_packets(df)
            df = self.adjust_df_for_calculated_data(df, main_dd_df)
        
        return df
    
    def adjust_df_for_segmented_packets(self, df: pd.DataFrame) -> pd.DataFrame:
        """This is specific to iterate over the main df and adjusts the packtes that have a flag for segmented SPs.
        0x3 is a unsegmented message, 0x1 is the first message, 0x0 is the middle, and 0x2 is the final segment.
        The way the function works is assuming the packets are in the right order (for each apid) from the file provided, and if it happens
        to have duplicate first or last values, it will skip one of the values. 
        """
        new_df_adjusted = pd.DataFrame()
        for apid in df.apid.unique():
            inner_df = (df.query(f"""apid == '{apid}'""")).copy(deep=True).reset_index(drop=True)

            if ['0x1'] not in inner_df['seq_flags'].unique():
                new_df_adjusted = pd.concat([new_df_adjusted, inner_df], ignore_index=True)
            else:
                start_of_message = False
                new_rows = pd.DataFrame(columns=df.columns)
                new_row_aux = pd.DataFrame(columns=df.columns)
            
                for i in range(0, len(inner_df)):
                    current_seq_flags = inner_df.iloc[i,:]['seq_flags']
                    if str(current_seq_flags) == '0x1' and not start_of_message:
                        start_of_message = True
                        new_row_aux.loc[i] = inner_df.iloc[i,:].copy(deep=True)
                    elif str(current_seq_flags) == '0x0' and start_of_message:
                        middle_data = inner_df.iloc[i,:]['data']
                        middle_pkt_data_length = inner_df.iloc[i,:]['pkt_data_length']
                        
                        current_pkt_data_length = new_row_aux['pkt_data_length'].item()
                        new_row_aux['pkt_data_length'] = hex(int(current_pkt_data_length, 16) + int(middle_pkt_data_length, 16))
                        
                        current_data = new_row_aux['data'].item()
                        new_row_aux['data'] = (current_data + middle_data)
                    elif str(current_seq_flags) == '0x2' and start_of_message:
                        start_of_message = False
                        final_data = inner_df.iloc[i,:]['data']
                        final_pkt_data_length = inner_df.iloc[i,:]['pkt_data_length']
                        
                        current_pkt_data_length = new_row_aux['pkt_data_length'].item()
                        new_row_aux['pkt_data_length'] = hex(int(current_pkt_data_length, 16) + int(final_pkt_data_length, 16))
                        
                        current_data = new_row_aux['data'].item()
                        new_row_aux['data'] = (current_data + final_data)

                        new_row_aux['seq_flags'] = '0x3'
                        new_rows.loc[i] = new_row_aux.iloc[0].copy(deep = True)
                        new_row_aux = pd.DataFrame(columns=df.columns)
                    
                new_df_adjusted = pd.concat([new_df_adjusted, new_rows.dropna(axis=1)], ignore_index=True, axis=0)
        
        return new_df_adjusted
    
    def read_through_hex_str(self, hex_string: str) -> list[dict]:
        """Goes through all the binary string and reads all the space packtes inside it."""
        binary_string = self.data_converter.hex_to_binary(hex_string)

        space_packet_list = []
        space_packet_bit_size = 0
        while space_packet_bit_size <= len(binary_string):
            space_packet, space_packet_bit_size = self.read_binary_str_to_space_packet(binary_string)
            binary_string = binary_string[space_packet_bit_size:]
            space_packet_list.append(space_packet)

        return space_packet_list

    def read_binary_str_to_space_packet(self, binary_string: str) -> tuple[dict, int]:
        """Main function to read the hex string. It will turn the hex into binary, then iterate over the components of
        the space packet and dinamically adjusts the bit size. Returns the space packet in a dict format with the components
        as binary strings.
        """
        number_of_bits_dict = self.space_packets.main_bits_dict.copy()
        
        space_packet_dict = dict()
        pointer = 0
        for component in number_of_bits_dict:
            number_of_bits_dict[component] = self.adjust_bit_size_for_variable_components(component, number_of_bits_dict, space_packet_dict)
            space_packet_dict[component], pointer = self.read_through_binary_str_and_update_pointer(binary_string, pointer, number_of_bits_dict[component])

        space_packet_bit_size = sum(number_of_bits_dict.values())
        return space_packet_dict, space_packet_bit_size

    def adjust_bit_size_for_variable_components(self, component: str, number_of_bits_dict: dict, space_packet_dict: dict) -> int:
        """Function to adjust the bit size for variable components. Returns the bit size for the component given as input.
        """
        if component == "secondary_header":
            try:
                bit_size = 64 if space_packet_dict['sec_hdr_flag'] == '1' else 0
            except KeyError as exc:
                raise KeyError("Attempted to access 'sec_hdr_flag' before it being available!") from exc
        elif component == "data":
            try:
                pkt_data_field_bits = (int(space_packet_dict['pkt_data_length'], 2) + 1)*8
                checksum_bits = number_of_bits_dict["checksum"]
                secondary_header_bits = number_of_bits_dict["secondary_header"]
                
                bit_size = pkt_data_field_bits - secondary_header_bits - checksum_bits
            except KeyError as exc:
                raise KeyError("Attempted to access 'pkt_data_length' or 'secondary_header' before it being available!") from exc
        else:
            bit_size = number_of_bits_dict[component]
        
        return bit_size

    def read_through_binary_str_and_update_pointer(self, binary_string: str, pointer: int, number_of_bits: int) -> tuple[str, int]:
        """Function to read an specific amount of bits from the binary string given the 
        starting point at the pointer vector. Updates the pointer to end of the bits read and returns the content
        read and the new pointer.
        """
        component_binary = binary_string[pointer:(pointer+number_of_bits)]
        new_pointer = pointer+number_of_bits

        return component_binary, new_pointer
    
    def adjust_df_for_calculated_data(self, df_in: pd.DataFrame, main_dd_df: pd.DataFrame) -> pd.DataFrame:

        assert not df_in.empty, "Empty df as input"
        assert all(column in df_in.columns for column in ['apid', 'data']), "Columns 'apid' and/or 'data' not found in DataFrame"

        df = df_in.copy(deep=True)
        new_fields = []
        for i in range(0, len(df)):
            apid = df.iloc[i,:]['apid']
            binary_data = df.iloc[i,:]['data']
            new_fields.append(self.calculate_data_conversion(apid, binary_data, main_dd_df))

        assert len(df) == len(new_fields)

        df['data_transformed'] = new_fields
        return df
    
    def calculate_data_conversion(self, apid:str, binary_data: int, main_dd_df: pd.DataFrame):
        """Method that given a apid, the corresponding binary_data for this apid and the main_df with the apid
        data types, it returns the calculated values in a data packet dict format"""
        formats_to_implement = []

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
                transformed_value = self.data_converter.binary_to_value(binary_slice, data_format, single_data_field['conversion'])
            except ValueError:
                print(f"Implement this format: {data_format} - {binary_slice}")
                if data_format not in formats_to_implement:
                    formats_to_implement.append(data_format)
                transformed_value = None

            single_data_field['value'] = transformed_value
            pointer += bit_length

        if len(formats_to_implement) > 0:
            print(f"These data formats need to be implemented {formats_to_implement} !")

        return new_data_packets
    
    def get_specific_apid_df_from_telemetry_df(self, apid:str, df_in: pd.DataFrame, main_dd_df: pd.DataFrame):
        """Method that given a specific apid, it will query the telemetry provided df and return another
        df with each field of the apid in a column."""
        assert apid in df_in['apid'].unique()
        
        apid_linked_dd = main_dd_df.query(f"""apid == '{apid}'""")['data_packets'].item()
        inner_df = df_in.query(f"""apid == '{apid}'""").copy(deep=True)
        
        new_df = inner_df['secondary_header'].reset_index().copy(deep=True)
        current_none_field = ''
        for field_id in range(0, len(apid_linked_dd)):
            y_aux = []
            for i in range(0, len(inner_df)):
                field_data = (inner_df.iloc[i, :]['data_transformed'])[field_id]
                if self._check_all_values_none(field_data, exclude_keys=['field']):
                    current_none_field = field_data['field']
                    
                field_name = field_data['field']
                field_unit = field_data['unit']
            
                if 'value' in field_data.keys():
                    y_aux.append(field_data['value'])
            if (field_name != 'Total') and not self._check_all_values_none(y_aux):
                if current_none_field == '':
                    column_name = f"{field_name} ({field_unit})"
                else:
                    column_name = f"{current_none_field} {field_name} ({field_unit})"
                new_df[column_name] = y_aux

        if self.is_datetime_column(new_df, 'secondary_header'):
            new_df = new_df.rename(columns={'secondary_header':'time'}).set_index('time')

        return new_df.drop(columns=['index'])

    def is_datetime_column(self, df: pd.DataFrame, column_name: str):
        assert column_name in df.columns
        return pd.api.types.is_datetime64_any_dtype(df[column_name])

    def _check_all_values_none(self, data, exclude_keys=None) -> bool:
        
        if exclude_keys is None:
            exclude_keys = []

        if isinstance(data, dict):
            is_empty = all(value is None for key, value in data.items() if key not in exclude_keys)
        elif isinstance(data, list):
            is_empty = all(value is None for value in data)
        else:
            raise NotImplementedError
        
        return is_empty
