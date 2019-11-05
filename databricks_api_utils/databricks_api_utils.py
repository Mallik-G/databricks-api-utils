from databricks_api import DatabricksAPI
import os
from base64 import b64decode, b64encode
from glob import glob


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


def import_dir(db_api_connection: DatabricksAPI, db_path: str, local_path: str = '.',
               format: str = 'SOURCE', source_format: str = '.py', **kwargs):
    """Imports all files of a certain format in a local directory to Databricks
    The Databricks workspace API only supports importing directories as .DBC
    This allows you to import a directory of files in source (or whatever) format
    """
    file_ext = format_to_extension(format, source_format)
    file_filterer = FileExtFilter(file_ext)
    file_language = extension_to_language(file_ext)

    files_to_import = list_source_files(local_path)
    files_to_import = [file for file in filter(file_filterer.filter, files_to_import)]

    for file in files_to_import:
        file_name = os.path.split(file)[-1]
        file_name = os.path.splitext(file_name)[0]
        db_file_path = os.path.join(db_path, file_name)
        import_file(db_api_connection, db_file_path, file, format, file_language, **kwargs)


def import_file(db_api_connection: DatabricksAPI, db_path: str, local_path: str = '.',
                format: str = 'SOURCE', language: str = 'PYTHON', **kwargs):
    file_content = open(local_path, 'rb').read()
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
    return _language_dict.get(extension, _language_dict['.py'])

#TODO: Rather than supplying a source format, default to export all source formats
def format_to_extension(format: str = 'SOURCE', source_format: str = '.py'):
    format_dict = {'SOURCE': source_format,
                   'JUPYTER': '.ipynb',
                   'HTML': '.html',
                   'DBC': '.dbc'}

    return format_dict.get(format, source_format)


_glob_dict = {True: '**/*.*',
              False: '*.*'}


def list_source_files(path: str = '.', file_ext: str = '.py', recursive: bool = True):
    """List all files of a given extension"""
    assert isinstance(recursive, bool), "expected recursive to be type bool"
    glob_path = glob_dict.get(recursive)

    files = glob(os.path.join(path, glob_path), recursive=recursive)
    file_filterer = FileExtFilter(file_ext)
    filtered_files = [file for file in filter(file_filterer.filter, files)]
    return filtered_files


class FileExtFilter:
    """Generates filter functions for a given file extension"""

    def __init__(self, file_ext: str):
        self.file_ext = file_ext

    def filter(self, item):
        _, item_ext = os.path.splitext(item)
        file_ext_comparison = self.file_ext == item_ext
        return file_ext_comparison
