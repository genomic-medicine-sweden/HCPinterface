#!/usr/bin/env python3

# Test for hcp.py, run from root in project directory tests/hcp_test.py

import os
import sys
import json
import argparse
import unittest
import tempfile

from HCPInterface import WD
from HCPInterface.hcp.hcp import HCPManager
from HCPInterface.hcp.helpers import calculate_etag

testWD = os.path.join(WD, '..', 'tests')
credentials_path = os.path.join(testWD, 'credentials.json')

hcpm = HCPManager(credentials_path=credentials_path)
hcpm.attach_bucket("ngs-test")

class TestProcess(unittest.TestCase):

    def test01_upload_file(self):
        self.assertIsNone(hcpm.upload_file(f"{testWD}/data/test_reads_R1.fasterq", "unittest/test_reads_R1.fasterq",silent=True))

    def test02_search_objects(self):
        self.assertEqual(hcpm.search_objects("unittest/test_reads_R1.fasterq")[0].key, "unittest/test_reads_R1.fasterq")

    def test03_download_file(self):
        obj = hcpm.get_object("unittest/test_reads_R1.fasterq")
        self.assertIsNone(hcpm.download_file(obj, "test_reads_R1.fasterq",silent=True,force=True))

    def test04_md5_sha256(self):
        remote_etag = hcpm.get_object("unittest/test_reads_R1.fasterq").e_tag
        calculated_etag = calculate_etag(f"{testWD}/data/test_reads_R1.fasterq")
        self.assertEqual(calculated_etag, remote_etag)

    def test05_delete_file(self):
        obj = hcpm.get_object("unittest/test_reads_R1.fasterq")
        self.assertIsNone(hcpm.delete_object(obj))
        self.assertIsNone(hcpm.get_object("unittest/test_reads_R1.fasterq")) # Verify that object was removed

    def test06_upload_error(self):
        self.assertRaises(TypeError, lambda:hcpm.upload_file())

    def test07_get_object_error(self):
        self.assertRaises(TypeError, lambda:hcpm.get_object())

    def test08_incorrect_credentials(self):
        self.assertRaises(ValueError, lambda:HCPManager("x", "x", "x"))


    @classmethod
    def tearDownClass(cls):
        # Remove the directories and files after the test is done.
        if "test_reads_R1.fasterq" in os.listdir():
            os.remove("test_reads_R1.fasterq")
