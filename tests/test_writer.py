import json
import unittest

from multiprocessing import Process
from time import sleep, time

import os

import h5py
import requests
from bsread.handlers.compact import Message, Value
from mflow import mflow

from sf_databuffer_writer import config, broker, writer
from sf_databuffer_writer.writer import audit_failed_write_request, process_message


class TestWriter(unittest.TestCase):
    TEST_OUTPUT_FILE = "ignore_output.h5"
    TEST_OUTPUT_FILE_ERROR = TEST_OUTPUT_FILE + ".err"
    TEST_AUDIT_FILE = "ignore_audit.txt"

    def setUp(self):
        config.DEFAULT_AUDIT_FILENAME = TestWriter.TEST_AUDIT_FILE
        self.data_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")

        self.channels = ["Channel1", "Channel2", "Channel3"]
        self.stream_output_port = 10000
        self.rest_port = 11000

        def fake_data(_, _2=None):
            test_data_file = os.path.join(self.data_folder, "dispatching_layer_sample.json")
            with open(test_data_file, 'r') as input_file:
                json_data = json.load(input_file)

            return json_data, 1001

        config.ERROR_IF_NO_DATA = False
        config.TRANSFORM_PULSE_ID_TO_TIMESTAMP_QUERY = False
        writer.get_data_from_buffer = fake_data

        self.broker_process = Process(target=broker.start_server, args=(self.channels,
                                                                        self.stream_output_port,
                                                                        100,
                                                                        self.rest_port))
        self.broker_process.start()
        sleep(1)

        self.n_pulses = len(fake_data("whatever")[0][0]["data"])

        def writer_start_server():
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

        try:
            os.remove(TestWriter.TEST_OUTPUT_FILE_ERROR)
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
        self.assertEqual(status, "stopped")

        requests.post("http://localhost:%d/parameters" % self.rest_port, json=parameters)
        requests.put("http://localhost:%d/start_pulse_id/%d" % (self.rest_port, start_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "receiving")

        requests.put("http://localhost:%d/stop_pulse_id/%d" % (self.rest_port, stop_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "stopped")

        # Wait for the chain to complete.
        sleep(2)

        file = h5py.File(TestWriter.TEST_OUTPUT_FILE)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/data"]), self.n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]), self.n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]), self.n_pulses)

        # Even channels with missing values should still have the same number of pulses.
        scalar_missing_data_n_values = 2
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/data"]), self.n_pulses)
        self.assertEqual(sum(file["data/SCALAR_MISSING_DATA/is_data_present"]), scalar_missing_data_n_values)

        # Even channels with missing values should still have the same number of pulses.
        array_missing_data_n_values = 3
        self.assertEqual(len(file["data/ARRAY_MISSING_DATA/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/ARRAY_MISSING_DATA/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/ARRAY_MISSING_DATA/data"]), self.n_pulses)
        self.assertEqual(sum(file["data/ARRAY_MISSING_DATA/is_data_present"]), array_missing_data_n_values)

        # Even channels with missing values should still have the same number of pulses.
        self.assertEqual(len(file["data/SCALAR_NO_DATA/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SCALAR_NO_DATA/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SCALAR_NO_DATA/data"]), self.n_pulses)
        self.assertEqual(sum(file["data/SCALAR_NO_DATA/is_data_present"]), 0)

        # Even channels with missing values should still have the same number of pulses.
        self.assertEqual(len(file["data/ARRAY_NO_DATA/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/ARRAY_NO_DATA/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/ARRAY_NO_DATA/data"]), self.n_pulses)
        self.assertEqual(sum(file["data/ARRAY_NO_DATA/is_data_present"]), 0)

        scalar_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]
        self.assertEqual(scalar_dataset.shape, (self.n_pulses, 1))
        self.assertEqual(str(scalar_dataset.dtype), "float32")

        array_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]
        self.assertEqual(array_dataset.shape, (self.n_pulses, 1024))
        self.assertEqual(str(array_dataset.dtype), "float32")

    def test_audit_failed_write_request(self):
        data_api_request = {"something": "wrong"}

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": self.TEST_OUTPUT_FILE}

        timestamp = time()

        audit_failed_write_request(data_api_request, parameters, timestamp)

        with open(self.TEST_OUTPUT_FILE_ERROR) as input_file:
            json_string = input_file.readlines()[0][18:]

        data = json.loads(json_string)

        err_data_api_request = json.loads(data["data_api_request"])
        err_parameters = json.loads(data["parameters"])
        err_timestamp = data["timestamp"]

        self.assertDictEqual(data_api_request, err_data_api_request)
        self.assertDictEqual(parameters, err_parameters)
        self.assertEqual(timestamp, err_timestamp)

    def test_adjusted_retrieval_delay(self):

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": self.TEST_OUTPUT_FILE}

        data_api_request = {'channels': [{'name': 'SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED'},
                                         {'name': 'SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX'},
                                         {'name': 'SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN'}],
                            'configFields': ['type', 'shape'],
                            'eventFields': ['channel', 'pulseId', 'value', 'shape', 'globalDate'],
                            'range': {'endPulseId': 5721143416, 'startPulseId': 5721143344},
                            'response': {'compression': 'none', 'format': 'json'}}

        # Simulate as if the request was generated 9 seconds ago.
        timestamp = time() - 9

        message = Message()
        message.data = {"parameters": Value(json.dumps(parameters)),
                        "data_api_request": Value(json.dumps(data_api_request)),
                        "timestamp": Value(timestamp)}

        mflow_message = mflow.Message(None, message)

        start_time = time()
        process_message(mflow_message, data_retrieval_delay=10)
        time_delta = time() - start_time

        # The call should last less than 3 second: 10 seconds delay, but request was generated 9 seconds ago.
        # Account some time for file writing.
        self.assertLess(time_delta, 3)

    def test_compact_file_format(self):
        start_pulse_id = 100
        stop_pulse_id = 200

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": TestWriter.TEST_OUTPUT_FILE,
                      "output_file_format": "compact"}

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "stopped")

        requests.post("http://localhost:%d/parameters" % self.rest_port, json=parameters)
        requests.put("http://localhost:%d/start_pulse_id/%d" % (self.rest_port, start_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "receiving")

        requests.put("http://localhost:%d/stop_pulse_id/%d" % (self.rest_port, stop_pulse_id))

        status = requests.get("http://localhost:%d/status" % self.rest_port).json()["status"]
        self.assertEqual(status, "stopped")

        # Wait for the chain to complete.
        sleep(2)

        file = h5py.File(TestWriter.TEST_OUTPUT_FILE)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/global_date"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/data"]), self.n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/global_date"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]), self.n_pulses)

        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/pulse_id"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/global_date"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/is_data_present"]), self.n_pulses)
        self.assertEqual(len(file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]), self.n_pulses)

        scalar_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MIN/data"]
        self.assertEqual(scalar_dataset.shape, (self.n_pulses, 1))
        self.assertEqual(str(scalar_dataset.dtype), "float32")

        array_dataset = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-CALIBRATED/data"]
        self.assertEqual(array_dataset.shape, (self.n_pulses, 1024))
        self.assertEqual(str(array_dataset.dtype), "float32")

        # Defined at the end of data/dispatching_layer_sample.json
        scalar_missing_data_n_values = 2
        scalar_missing_data_pulse_ids = [5721143360, 5721143380]
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/pulse_id"]), scalar_missing_data_n_values)
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/is_data_present"]), scalar_missing_data_n_values)
        self.assertEqual(len(file["data/SCALAR_MISSING_DATA/data"]), scalar_missing_data_n_values)
        self.assertListEqual(list(file["data/SCALAR_MISSING_DATA/pulse_id"]), scalar_missing_data_pulse_ids)
        self.assertListEqual(list(file["data/SCALAR_MISSING_DATA/is_data_present"]),
                             [True] * scalar_missing_data_n_values)

        # Defined at the end of data/dispatching_layer_sample.json
        self.assertEqual(len(file["data/SCALAR_NO_DATA/pulse_id"]), 0)
        self.assertEqual(len(file["data/SCALAR_NO_DATA/is_data_present"]), 0)
        self.assertEqual(len(file["data/SCALAR_NO_DATA/data"]), 0)
        self.assertListEqual(list(file["data/SCALAR_NO_DATA/data"].shape), [0, 1])

        # Defined at the end of data/dispatching_layer_sample.json
        self.assertEqual(len(file["data/ARRAY_NO_DATA/pulse_id"]), 0)
        self.assertEqual(len(file["data/ARRAY_NO_DATA/is_data_present"]), 0)
        self.assertEqual(len(file["data/ARRAY_NO_DATA/data"]), 0)
        self.assertListEqual(list(file["data/ARRAY_NO_DATA/data"].shape), [0, 2])

        # Test new global_date field.
        string_date = file["data/SAROP21-CVME-PBPS2:Lnk9Ch6-DATA-MAX/global_date"][0]
        # Expected format: '2018-06-08T14:04:51.551143344+02:00'
        self.assertTrue(isinstance(string_date, str))
