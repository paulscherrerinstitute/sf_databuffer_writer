import unittest

import os


class TestWriter(unittest.TestCase):
    TEST_OUTPUT_FILE = "ignore_output.h5"

    def tearDown(self):

        try:
            os.remove(TestWriter.TEST_OUTPUT_FILE)
        except:
            pass

    def test_write_data_to_file(self):
        dispatching_layer_data = {}
