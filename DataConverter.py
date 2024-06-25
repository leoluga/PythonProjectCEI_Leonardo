from datetime import datetime, timedelta
import pandas as pd

class DataConverter:

    def __init__(self) -> None:
        pass

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

    def gps_time_to_datetime(self, week, ms):
        """The week field indicates the unsigned integer number of weeks elapsed since the beginning of the current GPS epoch (which
        started on January 6, 1980). The ms field indicates the unsigned integer number of milliseconds
        elapsed since the beginning of the current week. Returns the datetime formatted date.
        """
        gps_epoch = datetime(1980, 1, 6)

        days = week * 7
        total_timedelta = timedelta(days=days, milliseconds=ms)

        result_datetime = gps_epoch + total_timedelta

        return result_datetime

