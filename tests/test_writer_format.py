import json
import unittest

import os

import h5py

from sf_databuffer_writer import config
from sf_databuffer_writer.writer import write_data_to_file


class TestWriter(unittest.TestCase):
    TEST_OUTPUT_FILE = "ignore_output.h5"

    def setUp(self):
        self.data_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")
        config.ERROR_IF_NO_DATA = False

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

        test_data_file = os.path.join(self.data_folder, "dispatching_layer_sample.json")
        with open(test_data_file, 'r') as input_file:
            json_data = json.load(input_file)

        n_pulses = len(json_data[0]["data"])

        write_data_to_file(parameters, json_data)

        file = h5py.File(TestWriter.TEST_OUTPUT_FILE)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/pulse_id"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/global_date"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/is_data_present"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/data"]), n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/pulse_id"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/global_date"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/is_data_present"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]), n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/global_date"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/is_data_present"]), n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]), n_pulses)

        scalar_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]
        self.assertEqual(scalar_dataset.shape, (n_pulses, 1))
        self.assertEqual(str(scalar_dataset.dtype), "float32")

        string_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/global_date"]
        self.assertEqual(string_dataset.shape, (n_pulses,))
        self.assertTrue(isinstance(string_dataset[0], str))

        array_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]
        self.assertEqual(array_dataset.shape, (n_pulses, 1024))
        self.assertEqual(str(array_dataset.dtype), "float32")

        pulse_id_start = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"][0]
        pulse_id_stop = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"][-1]

        # Pulse ids taken from the dispatching layer request above.
        self.assertTrue(5721143344 <= pulse_id_start)
        self.assertTrue(5721143416 >= pulse_id_stop)

    def test_write_camera_data(self):
        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": self.TEST_OUTPUT_FILE}

        test_data_file = os.path.join(self.data_folder, "camera_image_sample.json")
        with open(test_data_file, 'r') as input_file:
            json_data = json.load(input_file)

        n_pulses = len(json_data[0]["data"])

        write_data_to_file(parameters, json_data)

        file = h5py.File(TestWriter.TEST_OUTPUT_FILE)

        self.assertEqual(len(file["data/SARES20-PROF142-M1:FPICTURE/data"]), n_pulses)
        self.assertEqual(file["data/SARES20-PROF142-M1:FPICTURE/data"].shape, tuple([n_pulses] + [659, 494][::-1]))
