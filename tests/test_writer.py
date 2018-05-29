import json
import unittest

from multiprocessing import Process
from time import sleep

import os

import h5py
import requests

from sf_databuffer_writer import config, broker, writer


class TestWriter(unittest.TestCase):
    TEST_OUTPUT_FILE = "ignore_output.h5"
    TEST_AUDIT_FILE = "ignore_audit.txt"

    def setUp(self):
        config.DEFAULT_AUDIT_FILENAME = TestWriter.TEST_AUDIT_FILE
        self.data_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")

        self.channels = ["Channel1", "Channel2", "Channel3"]
        self.stream_output_port = 10000
        self.rest_port = 11000

        self.broker_process = Process(target=broker.start_server, args=(self.channels,
                                                                        self.stream_output_port,
                                                                        100,
                                                                        self.rest_port))
        self.broker_process.start()
        sleep(1)

        def fake_data(_):
            test_data_file = os.path.join(self.data_folder, "dispathing_layer_sample.json")
            with open(test_data_file, 'r') as input_file:
                json_data = json.load(input_file)

            return json_data

        def writer_start_server():
            writer.get_data_from_buffer = fake_data
            writer.start_server("tcp://localhost:%d" % self.stream_output_port)

        self.writer_process = Process(target=writer_start_server)
        self.writer_process.start()
        sleep(1)

    def tearDown(self):
        self.broker_process.terminate()
        self.writer_process.terminate()

        try:
            os.remove(TestWriter.TEST_AUDIT_FILE)
        except:
            pass

        try:
            os.remove(TestWriter.TEST_OUTPUT_FILE)
        except:
            pass

        sleep(1)

    def test_normal_interaction(self):
        start_pulse_id = 100
        stop_pulse_id = 200

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": TestWriter.TEST_OUTPUT_FILE}

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "waiting")

        requests.post("http://localhost:%d/parameters" % self.rest_port, json=parameters)
        requests.put("http://localhost:%d/start_pulse_id/%d" % (self.rest_port, start_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "writing")

        requests.put("http://localhost:%d/stop_pulse_id/%d" % (self.rest_port, stop_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "waiting")

        # Wait for the chain to complete.
        sleep(2)

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
