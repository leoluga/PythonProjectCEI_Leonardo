from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import struct

class DataConverter:
    """This class is to centralize all the conversion methods needed."""

    def __init__(self) -> None:
        pass

    def binary_to_value(self, binary_str, data_type, conversion_formula_for_adc = None):
        """Main converter for the class, centralizes a range of possible formats that it can read 
        and turn into the desired format.
        """
        if binary_str == '':
                return None
        if data_type is None:
            return None
        
        if 'uint' in data_type:
            return int(binary_str, 2)
        elif data_type == 'int16':
            return int(binary_str, 2) if binary_str[0] == '0' else int(binary_str, 2) - (1 << 16)
        elif data_type == 'int32':
            return int(binary_str, 2) if binary_str[0] == '0' else int(binary_str, 2) - (1 << 32)
        elif data_type == 'float':
            return struct.unpack('!f', struct.pack('!I', int(binary_str, 2)))[0]
        elif data_type == 'quaternion' or 'vector' in data_type:
            return [self.binary_to_value(binary_str[i:i+32], 'float') for i in range(0, len(binary_str), 32)]
        elif data_type == 'char':
            return chr(int(binary_str, 2))
        elif data_type == 'uchar':
            return int(binary_str, 2)
        elif data_type == 'bit':
            return int(binary_str)
        elif data_type == 'GPS time':
            return self.convert_64bit_binary_to_datetime(binary_str)
        elif 'matrix' in data_type:
            return self.binary_string_to_matrix(binary_str, data_type)
        elif data_type == 'bitfield':
            return binary_str
        elif data_type == 'css':
            return binary_str
        elif 'ADC' in data_type:
            return self.convert_binary_to_adc_value(binary_str, data_type, conversion_formula_for_adc)
        else:
            raise ValueError("Unsupported data type")

    def convert_binary_to_adc_value(self, binary_str, data_type, conversion_formula_for_adc):
        """A way of getting a typed formula from excel cell and evalueate the value from
        from it, given the ADC input as binary string.
        """
        if conversion_formula_for_adc is None or conversion_formula_for_adc == 'N/A':
                return None
        input_16bit = int(binary_str, 2)
        
        # Use mask to extract the bit ADC value from the 16-bit input (lower bits)
        if data_type == '12-bit ADC':
            adc_value = input_16bit & 0x0FFF
        elif data_type == '10-bit ADC':
            adc_value = input_16bit & 0x03FF

        conversion_formula = conversion_formula_for_adc
        conversion_formula = conversion_formula.replace("^", "**").replace("–", "-")
        conversion_formula = conversion_formula.replace("adc", str(adc_value))
        
        return eval(conversion_formula)

    def hex_to_binary(self, hex_string: str) -> str:
        """
        Function to convert hex string to a binary string with 4-bit representation
        Converts each hex char to integer, then format as 4-bit binary and adds it to
        a final binary string
        """
        binary_string = ""
        for char in hex_string:
            binary_string += format(int(char, 16), '04b')
        return binary_string

    def convert_64bit_binary_to_datetime(self, binary_string: str) -> datetime| pd._libs.tslibs.nattype.NaTType:
        """
        GPS time is composed of a 32-bit week field and a 32-bit ms field. This takes the week and milliseconds
        and transform them to datetime object.
        """
        if binary_string == '':
            return pd.NaT
        elif len(binary_string) != 64:
            raise ValueError("The binary string must be exactly 64 bits long")

        week_bits = binary_string[:32]
        ms_bits = binary_string[32:]

        week = int(week_bits, 2)
        ms = int(ms_bits, 2)

        return self.gps_time_to_datetime(week, ms)

    def gps_time_to_datetime(self, week, ms) -> datetime:
        """The week field indicates the unsigned integer number of weeks elapsed since the beginning of the current GPS epoch (which
        started on January 6, 1980). The ms field indicates the unsigned integer number of milliseconds
        elapsed since the beginning of the current week. Returns the datetime formatted date.
        """
        gps_epoch = datetime(1980, 1, 6)

        days = week * 7
        total_timedelta = timedelta(days=days, milliseconds=ms)

        result_datetime = gps_epoch + total_timedelta

        return result_datetime

    def binary_string_to_matrix(self, binary_string: str, format_str: str) -> np.ndarray:
        """Transforms a binary string to matrix format provided, format is of type,for example, 'matrix3' for a 3x3 matrix
        or 'matrix43' for a 4x3 matrix."""
        rows, cols = self.get_row_and_columns_from_format(format_str)
        bits_per_entry = len(binary_string)/(rows*cols)

        assert bits_per_entry == int(bits_per_entry), "Not possible to split the binary string into equal size chunks!"
    
        bits_per_entry = int(bits_per_entry)
        chunks = [binary_string[i:i + bits_per_entry] for i in range(0, len(binary_string), bits_per_entry)]
        matrix_values = [self.binary_to_value(chunk, 'float') for chunk in chunks]

        matrix = np.array(matrix_values).reshape(rows, cols)
        return matrix
    
    def get_row_and_columns_from_format(self, format_str: str) -> tuple[int, int]:
        """Given the format it retrieves the row and columns from it."""
        assert 'matrix' in format_str or 'Matrix' in format_str

        format_str = format_str.strip().lower()
        row_and_columns = format_str.split('matrix')[-1]

        if len(row_and_columns) == 0:
            raise ValueError("Format string does not have the values for row and columns")
        elif len(row_and_columns) == 1:
            rows, cols = int(row_and_columns), int(row_and_columns)
        elif len(row_and_columns) == 2:
            rows, cols = int(row_and_columns[0]), int(row_and_columns[1])
        else:
            raise ValueError("Too many values to unpack!")

        return rows, cols


