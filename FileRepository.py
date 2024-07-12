import os
import shutil
import pandas as pd
import ezodf
from pyexcel_ods import get_data

class FileRepository:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def list_files(self):
        """List all files in the repository."""
        return os.listdir(self.folder_path)
    
    def read_ods_document_by_sheet(self, file_name, sheet_name):
        """reads specific sheet from document."""
        ods = self.read_ods(file_name)
        sheet = ods.sheets[sheet_name]
        
        data = []
        for row in sheet.rows():
            data_row = [cell.value for cell in row]
            data.append(data_row)
        
        df = pd.DataFrame(data)
        return df
    
    def get_specific_sheet_names_from_ods_document(self, file_name: str, starts_with: str) -> list:
        """Filter the sheets based on the first characters."""
        ods = self.read_ods(file_name)
        sheets = [name for name in ods.sheets.names() if name.startswith(starts_with)]

        return sheets
    
    def read_ods(self, file_name: str):
        _, ext = os.path.splitext(file_name)
        assert ext == '.ods', "File must be of extension '.ods'!"
        file_path = self.get_file_path_from_file_name(file_name)
        ods_file = ezodf.opendoc(file_path)

        return ods_file
    
    def read_telemetry_dump_file(self, file_name: str) -> str:
        """Attempts to read a normal hex file, if error it will read as if it were a binary file.
        Returns the hex data in str one line format."""
        try:
            hex_data = self.read_hex_file_to_hex_str(file_name)
        except UnicodeDecodeError:
            hex_data = self.read_binary_file_to_hex(file_name)
        return hex_data

    def read_hex_file_to_hex_str(self, file_name: str) -> str:
        """This will take a file with hex strings and join them all into one line."""
        file_path = self.get_file_path_from_file_name(file_name)
        
        with open(file_path, 'r') as f:
            all_lines = ''.join(line.strip() for line in f)
        return all_lines
    
    def read_binary_file_to_hex(self, file_name: str) -> str:
        """This will read a binary format file and turn it into a one line hex string."""
        file_path = self.get_file_path_from_file_name(file_name)

        with open(file_path, 'rb') as file:
            binary_data = file.read()
        hex_data = binary_data.hex()
        return hex_data

    def dump_file(self, file_name, content):
        """Dump content into a file in the repository."""
        file_path = self.get_file_path_from_file_name(file_name)
        _, ext = os.path.splitext(file_name)
        
        if ext == '.ods':
            raise NotImplementedError("Writing to .ods files is not implemented.")
        elif ext == '.csv':
            content.to_csv(file_path, index=False)
        elif ext in ['.xlsx', '.xls']:
            content.to_excel(file_path, index=False)
        else:
            with open(file_path, 'w') as file:
                file.write(content)

    def upload_file(self, file, file_name=None):
        """Upload a file to the repository."""
        if file_name is None:
            file_name = file.filename
        dest_file_path = os.path.join(self.folder_path, file_name)
        with open(dest_file_path, 'wb') as dest_file:
            shutil.copyfileobj(file, dest_file)


    def get_file_path_from_file_name(self, file_name):
        assert file_name in self.list_files(), "File must exist within the repository folder!"
        file_path = os.path.join(self.folder_path, file_name)

        return file_path
