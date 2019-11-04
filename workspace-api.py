import requests as req
import os
import base64
import json
import sys
import pandas as pd
import glob

db_instance = "https://australiaeast.azuredatabricks.net/"
db_api_base = f"{db_instance}api/2.0/"
db_pat = os.environ['DATABRICKS-WORKSPACE-API-PAT'].encode("UTF-8")

def db_api_get(endpoint: str, body: dict):
    response = req.get(f"{db_api_base}{endpoint}",
                       headers={"Authorization": b"Basic " + base64.standard_b64encode(b"token:" + db_pat)},
                       json = body)

    return response

def db_api_post(endpoint: str, body: dict):
    response = req.post(f"{db_api_base}{endpoint}",
                       headers={"Authorization": b"Basic " + base64.standard_b64encode(b"token:" + db_pat)},
                       json = body)

    return response

#Get a list of all shared files
ws_list =  db_api_get('workspace/list', {'path': '/Shared/'})
ws_list.json()

#Create a project


#Export a project (from Databricks to local)
#This only supports DBC file
dl_response = db_api_get('workspace/export',
                         {"path": "/Users/sam.harvey@ea.govt.nz/workspace-api-test",
                          "format": "DBC",
                          "direct_download": 'true'})

with open('test.dbc', 'wb') as file:
    file.write(dl_response.content)

#Export all files as source (from Databricks to local)
file_list =  db_api_get('workspace/list/', {'path': '/Users/sam.harvey@ea.govt.nz/workspace-api-test'})

ws_objects = file_list.json()["objects"]
ws_object_paths = [db_object['path'] for db_object in ws_objects]

def language_to_extension(language: str):
    if language == "PYTHON":
        return ".py"

def extension_to_language(extension: str):
    if extension == "py":
        return "PYTHON"

for i in ws_objects:
    file_name = i['path'].split("/")[-1]
    file_language = i['language']
    file_ext = language_to_extension(file_language)

    dl_response = db_api_get(f'workspace/export',
                             {"path": i,
                              "format": "source",
                              "direct_download": 'true'})

    with open(f'{file_name}{file_ext}', 'wb+') as file:
        file.write(dl_response.content)

#Import all files (from Databricks to local)
files_to_import =  glob.glob("*.py", recursive=True)

#Push files into Databricks
for i in files_to_import:
    file_name = i.split(".")[0]
    file_ext = i.split(".")[-1]
    file_language = extension_to_language(file_ext)

    file_content = open(i, 'rb').read()
    file_content = base64.standard_b64encode(file_content).decode()

    dl_response = db_api_post(f'workspace/import',
                              {'path': f'/Users/sam.harvey@ea.govt.nz/workspace-api-test/{file_name}',
                               'language': file_language,
                               "content": file_content,
                               'overwrite': 'true',
                               'format': 'source'})