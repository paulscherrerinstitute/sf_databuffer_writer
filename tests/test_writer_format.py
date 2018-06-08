import json
import unittest

import os

import h5py

from sf_databuffer_writer.writer import write_data_to_file


class TestWriter(unittest.TestCase):
    TEST_OUTPUT_FILE = "ignore_output.h5"

    def setUp(self):
        self.data_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")

    def tearDown(self):

        try:
            os.remove(TestWriter.TEST_OUTPUT_FILE)
        except:
            pass

    def test_write_data_to_file(self):

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": self.TEST_OUTPUT_FILE}

        # Request used to retrieve the the sample data.
        # {'channels': ['SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED',
        #               'SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX',
        #               'SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN'],
        #  'configFields': ['type', 'shape'],
        #  'eventFields': ['channel', 'pulseId', 'value', 'shape'],
        #  'mapping': {'incomplete': 'fill-null'},
        #  'range': {'endPulseId': 5633494120, 'startPulseId': 5633493420},
        #  'response': {'compression': 'none', 'format': 'json'}}

        test_data_file = os.path.join(self.data_folder, "dispathing_layer_sample.json")
        with open(test_data_file, 'r') as input_file:
            json_data = json.load(input_file)

        write_data_to_file(parameters, json_data)

        file = h5py.File(TestWriter.TEST_OUTPUT_FILE)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/pulse_id"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/is_data_present"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/data"]), 36)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/pulse_id"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/is_data_present"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]), 36)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/is_data_present"]), 36)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]), 36)

        scalar_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]
        self.assertEqual(scalar_dataset.shape, (36, 1))
        self.assertEqual(str(scalar_dataset.dtype), "float32")

        array_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]
        self.assertEqual(array_dataset.shape, (36, 1024))
        self.assertEqual(str(array_dataset.dtype), "float32")

        pulse_id_start = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"][0]
        pulse_id_stop = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"][-1]

        # Pulse ids taken from the dispatching layer request above.
        self.assertTrue(5633493420 <= pulse_id_start)
        self.assertTrue(5633494120 >= pulse_id_stop)
