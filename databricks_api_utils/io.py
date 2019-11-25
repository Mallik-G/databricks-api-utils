from databricks_api import DatabricksAPI
import os
from base64 import b64decode, b64encode
from warnings import warn
from pathlib import Path
from requests.exceptions import HTTPError

def export_dir(db_api_connection: DatabricksAPI, db_path: str, local_path: str, recursive: bool = True, **kwargs):
    """Exports a directory from Databricks to local.
    The Databricks workspace API only supports exporting directories as .DBC
    This allows you to export a directory of files in source (or whatever) format
    """
    files_to_export = db_api_connection.workspace.list(path=db_path)
    files_to_export = files_to_export['objects']

    for file in files_to_export:
        if file['object_type'] not in ['DIRECTORY', 'NOTEBOOK']:
            warn(f"file: {file['path']} of object type {file['object_type']} cannot be exported.\n"
                 + f"{file['object_type']} is not supported by the Databricks Workspace API")

    if recursive:
        to_parse = [i for i in files_to_export if i['object_type'] == 'DIRECTORY']
        files_to_export = [i for i in files_to_export if i['object_type'] == 'NOTEBOOK']

        while len(to_parse) > 0:
            parse = to_parse.pop()
            sub_obj = db_api_connection.workspace.list(parse['path'])['objects']

            for i in sub_obj:
                if i['object_type'] == 'DIRECTORY':
                    to_parse.append(i)
                elif i['object_type'] == 'NOTEBOOK':
                    files_to_export.append(i)
                else:
                    warn(f"file: {i['path']} of object type {i['object_type']} cannot be exported")

        for file in files_to_export:
            file['db_relative_path'] = Path(file['path']).relative_to(db_path).parent
            file_local_path = Path(local_path).joinpath(file['db_relative_path'])
            file_path = file['path']

            if not os.path.exists(file_local_path):
                os.makedirs(file_local_path, exist_ok=True)

            export_file(db_api_connection, file_path, file_local_path, **kwargs)

    else:
        files_to_export = [i for i in files_to_export if i['object_type'] == 'NOTEBOOK']

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


def list_relative_file_paths(path: str = '.', recursive: bool = True):
    """Returns a list of relative file paths to all files in a given directory"""
    if recursive:
        path_gen = Path(path).glob("**/*")
    else:
        path_gen = Path(path).glob("*")

    paths = [f"{i.parent}/{i.name}" for i in path_gen if i.is_file()]
    return paths


def path_to_db_path(path: Path):
    """Converts a pathlib.Path to a DBFS path"""
    db_path = str(path).replace("\\", "/")
    db_path = os.path.splitext(db_path)[0]
    return db_path


def import_dir(db_api_connection: DatabricksAPI, db_path: str, local_path: str = '.',
               format: str = 'SOURCE', recursive: bool = True, **kwargs):
    """Imports all files of a certain format in a local directory to Databricks
    The Databricks workspace API only supports importing directories as .DBC
    This allows you to import a directory of files in source (or whatever) format
    """
    file_filterer = FileFormatTypeFilter(format)
    files_to_import = list_relative_file_paths(local_path, recursive)
    files_to_import = [file for file in filter(file_filterer.filter, files_to_import)]

    # Create subdirectories for imports.
    # Importing with a directory name included gives a file name like 'dir/file' in DBFS
    if recursive:
        db_dirs = [Path(db_path).joinpath(Path(file).relative_to(local_path)).parent for file in files_to_import]
        db_dirs = [path_to_db_path(i) for i in db_dirs]
        for db_dir in db_dirs:
            try:
                db_api_connection.workspace.mkdirs(db_dir)
            # Pass errors from trying to create a directory that exists already
            except HTTPError as error:
                warn(error)
                pass

    if len(files_to_import) == 0:
        warn(f"No files in {format} format located at {local_path}.")

    for file in files_to_import:
        file_path = Path(file)
        file_name = file_path.name
        _, file_ext = os.path.splitext(file_name)[0:2]
        file_language = extension_to_language(file_ext)
        db_file_path = path_to_db_path(Path(db_path).joinpath(file_path.relative_to(local_path)))
        import_file(db_api_connection, db_file_path, file_path, format, file_language, **kwargs)


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

