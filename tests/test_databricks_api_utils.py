#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `databricks_api_utils` package."""

import unittest
import os
from databricks_api_utils import databricks_api_utils
from shutil import rmtree

testing_dir = '/tmp-db-api-utils/'
local_test_dir = '/tests/local-temp/'
lang_examples = 'tests/fixtures/lang_examples'
project_dir = 'tests/fixtures/project_example'


def get_dir_contents_dict(path: str):
    """Get dict of file contents from a directory. Used for comparison of before and after of files in DBFS"""
    files = databricks_api_utils.list_relative_file_paths(path)
    file_names = [os.path.split(file)[-1] for file in files]
    file_contents = []
    for file in files:
        with open(file, 'rb') as file_con:
            file_contents.append(file_con.read())
    return dict(zip(file_names, file_contents))


class TestDatabricksApiUtils(unittest.TestCase):
    """Tests for `databricks_api_utils` package."""

    @classmethod
    def setUpClass(cls):
        cls.db_instance = os.environ['DATABRICKS_INSTANCE']
        cls.db_token = os.environ['DATABRICKS_TOKEN']
        cls.db_api = databricks_api_utils.DatabricksAPI(host=cls.db_instance, token=cls.db_token)
        cls.db_api.workspace.mkdirs(testing_dir)

        os.makedirs(local_test_dir, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures, if any."""
        cls.db_api.workspace.delete(testing_dir, recursive=True)

        rmtree(local_test_dir)

    def setUp(self):
        """Set up test fixtures, if any."""
        if not self.db_api.client.verify:
            self.fail('client not verified')

    def test_import_formats(self):
        """Test that we can import in all supported formats"""
        for format in databricks_api_utils._format_dict.keys():
            try:
                databricks_api_utils.import_dir(self.db_api, testing_dir, lang_examples, format)
            except:
                self.fail(f"could not import format {format}")

    def test_expected_exports(self):
        """Test that when we import/export a project directory to/from DBFS the file contents are unchanged"""
        databricks_api_utils.import_dir(self.db_api, testing_dir, project_dir)
        databricks_api_utils.export_dir(self.db_api, testing_dir, local_test_dir)

        fixture_file_contents = get_dir_contents_dict(project_dir)
        imported_file_contents = get_dir_contents_dict(local_test_dir)

        self.assertDictEqual(fixture_file_contents,
                             imported_file_contents)
