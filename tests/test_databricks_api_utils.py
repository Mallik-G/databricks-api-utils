#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `databricks_api_utils` package."""


import unittest
import os
from databricks_api_utils import databricks_api_utils


class TestDatabricksApiUtils(unittest.TestCase):
    """Tests for `databricks_api_utils` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.db_instance = os.environ['DATABRICKS_INSTANCE']
        self.db_token = os.environ['DATABRICKS_TOKEN']
        self.db_api = databricks_api_utils.DatabricksAPI(host=self.db_instance, token=self.db_token)

        if self.db_api.client.verify:
            self.db_api.workspace.mkdirs('/tmp/')

    def tearDown(self):
        """Tear down test fixtures, if any."""
        if self.db_api.client.verify:
            self.db_api.workspace.delete('/tmp/', recursive=True)



    def test_databricks_connection(self):
        """That our connection is working."""
        self.assertTrue(self.db_api.client.verify)

    def test_databricks_import(self):
        self.assertTrue(True)

