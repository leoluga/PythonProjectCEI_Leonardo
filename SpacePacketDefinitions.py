
import numpy as np

MAIN_BITS_DICT = {
    "version_number":3,
    "pkt_type":1,
    "sec_hdr_flag":1,
    "apid":11,
    "seq_flags":2,
    "seq_count":14,
    "pkt_data_length":16,
    "secondary_header": None,
    "data":None,
    "checksum":16
}

class SpacePacketDefinitions:
    """This class will centralize the main CCSDS definitions of the project."""
    main_bits_dict: dict

    def __init__(self) -> None:
        self.main_bits_dict = MAIN_BITS_DICT

    def single_data_field_dict(
            self,
            field_name: str, 
            length_bits: int, 
            format_str: str, 
            nominal_min: int| float | None, 
            nominal_max: int| float | None, 
            conversion: str | None, 
            unit: str | None,
            observation = None
        ) -> dict:

        single_data_dict = {
            "field": field_name,
            "lenght(bits)": length_bits,
            "format": format_str,
            "nominal_minimum": nominal_min,
            "nominal_maximum": nominal_max,
            "conversion": conversion,
            "unit": unit,
            "observation": observation
        }

        return single_data_dict

    def single_data_field_dict_from_row(self, row: np.ndarray | list) -> dict:
        """a data field has to have at least the field, bit length and format, and it varies how much columns it has."""

        assert len(row) >= 3
        field_name, bit_length, format_str = row[0], row[1], row[2]

        if len(row) >= 8:
            nominal_min, nominal_max, conversion, unit, observation = row[3], row[4], row[5], row[6], row[7]
        elif len(row) == 7:
            nominal_min, nominal_max, conversion, unit, observation = row[3], row[4], row[5], row[6], None
        elif len(row) == 6:
            nominal_min, nominal_max, conversion, unit, observation = row[3], row[4], 'N/A', 'N/A', row[-1]
        elif len(row) == 5:
            nominal_min, nominal_max, conversion, unit, observation = row[3], row[4], 'N/A', 'N/A', None
        elif len(row) == 4:
            nominal_min, nominal_max, conversion, unit, observation = 'N/A', 'N/A', 'N/A', 'N/A', row[-1]
        else:
            nominal_min, nominal_max, conversion, unit, observation = 'N/A', 'N/A', 'N/A', 'N/A', None

        try:
            field_name = field_name.strip()
        except AttributeError:
            field_name = ''

        return self.single_data_field_dict(
            field_name, 
            bit_length, 
            format_str, 
            nominal_min, 
            nominal_max, 
            conversion, 
            unit, 
            observation
        )