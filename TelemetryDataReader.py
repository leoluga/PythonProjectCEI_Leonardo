
import pandas as pd

from DataConverter import DataConverter
from SpacePacketDefinitions import SpacePacketDefinitions

class TelemetryDataReader:

    data_converter: DataConverter 
    space_packets: SpacePacketDefinitions

    def __init__(self) -> None:
        self.data_converter = DataConverter()
        self.space_packets = SpacePacketDefinitions()

    def get_space_packets_df_from_file(self, file_path: str, transform_binary_values: bool = True) -> pd.DataFrame:
        """Easier way to get the df directly from the file_path."""
        space_packets = self.read_file_and_get_space_packets(file_path)
        df = self.create_df_from_space_packets(space_packets, transform_binary_values)
        return df
    
    def read_file_and_get_space_packets(self, file_path: str) -> list[dict]:
        """Attempts to read a normal hex file, if error it will read as if it were a binary file.
        After being able to read it will parse the hex string into space packets format."""
        try:
            hex_data = self.read_hex_file_to_hex_str(file_path)
        except UnicodeDecodeError:
            hex_data = self.binary_file_to_hex(file_path)
        
        packets = self.read_through_hex_str(hex_data)

        return packets

    def create_df_from_space_packets(self, packets: list[dict], transform_binary_values: bool = True) -> pd.DataFrame:
        """Allows the space packets to be displayes as df format, also it performs transformations to the binary values of
        a space packet."""
        df = pd.DataFrame(packets)
        if transform_binary_values:
            df['version_number'] = df['version_number'].apply(lambda x: int(x, 2))
            df['apid'] = df['apid'].apply(lambda x: hex(int(x, 2)))
            df['seq_flags'] = df['seq_flags'].apply(lambda x: hex(int(x, 2)))
            df['pkt_data_length'] = df['pkt_data_length'].apply(lambda x: hex(int(x, 2)))
            df['secondary_header'] = df['secondary_header'].apply(self.data_converter.convert_64bit_binary_to_datetime)
            
        return df
    
    def read_hex_file_to_hex_str(self, file_path: str) -> str:
        """This will take a file with hex strings and join them all into one line."""
        with open(file_path, 'r') as f:
            all_lines = ''.join(line.strip() for line in f)
        return all_lines
    
    def binary_file_to_hex(self, file_path: str) -> str:
        """This will read a binary format file and turn it into a one line hex string."""
        with open(file_path, 'rb') as file:
            binary_data = file.read()
        hex_data = binary_data.hex()
        return hex_data

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