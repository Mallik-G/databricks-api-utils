#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `databricks_api_utils` package."""

import unittest
import os
from databricks_api_utils import io
from shutil import rmtree
from pathlib import Path
import time
from random import random

testing_dir = '/tmp-db-api-utils' + str(int(random()*1e8))
local_test_dir = './tests/local-temp/'
lang_examples = 'tests/fixtures/lang_examples'
project_dir = 'tests/fixtures/project_example'
recursion_dir = 'tests/fixtures/recursion_example'

# This only has one extension for S
_extension_dict = {v: k for k, v in io._language_dict.items()}


def get_dir_contents_dict(path: str):
    """Get dict of file contents from a directory. Used for comparison of before and after of files in DBFS"""
    files = io.list_relative_file_paths(path)
    file_names = [os.path.split(file)[-1] for file in files]
    file_contents = []
    for file in files:
        with open(file=file, mode='r', newline=None) as file_con:
            file_contents.append(file_con.read())
    return dict(zip(file_names, file_contents))


class TestDatabricksApiUtils(unittest.TestCase):
    """Tests for `databricks_api_utils` package."""

    @classmethod
    def setUpClass(cls):
        cls.db_instance = os.environ['DATABRICKS_INSTANCE']
        cls.db_token = os.environ['DATABRICKS_TOKEN']
        cls.db_api = io.DatabricksAPI(host=cls.db_instance, token=cls.db_token)

    def setUp(self):
        if not self.db_api.client.verify:
            self.fail('client not verified')

        os.makedirs(local_test_dir, exist_ok=True)
        self.db_api.workspace.mkdirs(testing_dir)

    def tearDown(self):
        self.db_api.workspace.delete(testing_dir, recursive=True)
        rmtree(local_test_dir, ignore_errors=True)
        # Need enough time for Databricks to delete things
        time.sleep(2.5)

    def test_import_formats(self):
        """Test that we can import in all supported formats"""
        for format in io._format_dict.keys():
            try:
                io.import_dir(self.db_api, testing_dir, lang_examples, format)
            except:
                self.fail(f"could not import format {format}")

    def test_expected_exports(self):
        """Test that when we import/export a project directory to/from DBFS the file contents are unchanged"""
        io.import_dir(self.db_api, testing_dir, project_dir)
        io.export_dir(self.db_api, testing_dir, local_test_dir)

        fixture_file_contents = get_dir_contents_dict(project_dir)
        imported_file_contents = get_dir_contents_dict(local_test_dir)

        self.assertDictEqual(fixture_file_contents,
                             imported_file_contents)

    def test_recursive_import(self):
        """Test that we preserve directory structure with recursive imports"""
        io.import_dir(db_api_connection=self.db_api, db_path=testing_dir, local_path=recursion_dir, recursive=True)
        test_dir = self.db_api.workspace.list(testing_dir)
        test_obj = test_dir['objects']

        # Get the locations of the imported files in DBFS
        to_parse = []
        for i in test_obj:
            if i['object_type'] == 'DIRECTORY':
                to_parse.append(i)

        while len(to_parse) > 0:
            parse = to_parse.pop()
            sub_obj = self.db_api.workspace.list(parse['path'])['objects']

            for i in sub_obj:
                if i['object_type'] == 'DIRECTORY':
                    to_parse.append(i)
                else:
                    test_obj.append(i)

        # Find all of these relative to the import path and add language extensions
        dbfs_dir_structure = [f"{Path(i['path']).relative_to(testing_dir).parent}/"
                              + f"{Path(i['path']).relative_to(testing_dir).name}"
                              + f"{_extension_dict.get(i.get('language'), '')}"
                              for i in test_obj]

        # Get the locations of the local files
        test_dir_structure = [f"{i.relative_to(recursion_dir).parent}/{i.relative_to(recursion_dir).name}" for i in
                              Path(recursion_dir).glob('**/*')]

        self.assertSetEqual(set(test_dir_structure), set(dbfs_dir_structure))

    def test_recursive_export(self):
        """Test that recursive export preserves dir structure"""
        io.import_dir(db_api_connection=self.db_api, db_path=testing_dir, local_path=recursion_dir, recursive=True)
        io.export_dir(db_api_connection=self.db_api, db_path=testing_dir, local_path=local_test_dir, recursive=True)

        original = [i.relative_to(recursion_dir) for i in Path(recursion_dir).glob("**/*")]
        exported = [i.relative_to(local_test_dir) for i in Path(local_test_dir).glob("**/*")]

        self.assertSetEqual(set(original), set(exported))
