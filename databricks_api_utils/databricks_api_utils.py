from databricks_api import DatabricksAPI
import os
from base64 import b64decode, b64encode
from warnings import warn


def export_dir(db_api_connection: DatabricksAPI, db_path: str, local_path: str, **kwargs):
    """Exports a directory from Databricks to local.
    The Databricks workspace API only supports exporting directories as .DBC
    This allows you to export a directory of files in source (or whatever) format
    """
    files_to_export = db_api_connection.workspace.list(path=db_path)
    files_to_export = files_to_export['objects']

    for file in files_to_export:
        file_path = file['path']
        export_file(db_api_connection, file_path, local_path, **kwargs)


def export_file(db_api_connection: DatabricksAPI, file_path: str, local_path: str, format: str = 'SOURCE'):
    file_name = os.path.split(file_path)[-1]
    file_export = db_api_connection.workspace.export_workspace(path=file_path, format=format)
    local_file_path = os.path.join(local_path, f"{file_name}.{file_export['file_type']}")

    file_content = b64decode(file_export['content'])

    with open(local_file_path, 'wb+') as local_file:
        local_file.write(file_content)


def list_relative_file_paths(path: str):
    """Returns a list of relative file paths to all files in a given directory"""
    file_paths = [os.path.join(os.path.relpath(path), file) for file in os.listdir(path)]
    return [file for file in filter(os.path.isfile, file_paths)]


def import_dir(db_api_connection: DatabricksAPI, db_path: str, local_path: str = '.',
               format: str = 'SOURCE', **kwargs):
    """Imports all files of a certain format in a local directory to Databricks
    The Databricks workspace API only supports importing directories as .DBC
    This allows you to import a directory of files in source (or whatever) format
    """
    file_filterer = FileFormatTypeFilter(format)
    files_to_import = list_relative_file_paths(local_path)
    files_to_import = [file for file in filter(file_filterer.filter, files_to_import)]

    if len(files_to_import) == 0:
        warn(f"No files in {format} format located at {local_path}.")

    for file in files_to_import:
        file_name = os.path.split(file)[-1]
        file_name, file_ext = os.path.splitext(file_name)[0:2]
        file_language = extension_to_language(file_ext)
        db_file_path = os.path.join(db_path, file_name)
        import_file(db_api_connection, db_file_path, file, format, file_language, **kwargs)


def import_file(db_api_connection: DatabricksAPI, db_path: str, local_path: str = '.',
                format: str = 'SOURCE', language: str = 'PYTHON', **kwargs):
    with open(local_path, 'rb') as file:
        file_content = file.read()
    file_content = b64encode(file_content).decode()

    db_api_connection.workspace.import_workspace(
        path=db_path,
        format=format,
        content=file_content,
        language=language,
        **kwargs
    )


_language_dict = {'.sc': 'SCALA',
                  '.scala': 'SCALA',
                  '.py': 'PYTHON',
                  '.r': 'R',
                  '.sql': 'SQL'}


def extension_to_language(extension: str = '.py'):
    return _language_dict.get(extension)


_format_dict = {'SOURCE': ['.py', '.r', '.scala', '.sc', '.sql'],
                'JUPYTER': '.ipynb',
                'HTML': '.html',
                'DBC': '.dbc'}


class FileFormatTypeFilter:
    def __init__(self, format: str):
        self.format_type = _format_dict.get(format)

    def filter(self, item):
        _, item_ext = os.path.splitext(item)
        file_format_comparison = item_ext.lower() in self.format_type
        return file_format_comparison

