#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `databricks_api_utils` package."""

import unittest
import os
from databricks_api_utils import databricks_api_utils


testing_dir = '/tmp-db-api-utils/'


class TestDatabricksApiUtils(unittest.TestCase):
    """Tests for `databricks_api_utils` package."""

    @classmethod
    def setUpClass(cls):
        cls.db_instance = os.environ['DATABRICKS_INSTANCE']
        cls.db_token = os.environ['DATABRICKS_TOKEN']
        cls.db_api = databricks_api_utils.DatabricksAPI(host=cls.db_instance, token=cls.db_token)
        cls.db_api.workspace.mkdirs(testing_dir)

    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures, if any."""
        if cls.db_api.client.verify:
            cls.db_api.workspace.delete(testing_dir, recursive=True)

    def setUp(self):
        """Set up test fixtures, if any."""
        if not self.db_api.client.verify:
            self.fail('client not verified')

    # def tearDown(self):
    #     """Tear down test fixtures, if any."""
    #     if self.db_api.client.verify:
    #         self.db_api.workspace.delete(testing_dir, recursive=True)

    def test_databricks_import(self):
        self.assertTrue(True)
