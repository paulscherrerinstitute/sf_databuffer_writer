import unittest

from multiprocessing import Process
import json
from time import sleep

import os
import requests
from bsread import source, PULL

from sf_databuffer_writer import config, broker


class TestBroker(unittest.TestCase):
    TEST_AUDIT_FILE = "ignore_output.txt"

    def setUp(self):
        config.DEFAULT_AUDIT_FILENAME = TestBroker.TEST_AUDIT_FILE

        self.channels = ["Channel1", "Channel2", "Channel3"]
        self.stream_output_port = 10000
        self.rest_port = 11000

        self.broker_process = Process(target=broker.start_server, args=(self.channels,
                                                                        self.stream_output_port,
                                                                        100,
                                                                        self.rest_port))

        self.broker_process.start()
        sleep(1)

    def tearDown(self):
        self.broker_process.terminate()

        try:
            os.remove(TestBroker.TEST_AUDIT_FILE)
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
                      "output_file": "test.h5"}

        with source(host="localhost", port=self.stream_output_port, mode=PULL, receive_timeout=500) as input_stream:
            message = input_stream.receive()
            self.assertIsNone(message)

            requests.post("http://localhost:%d/parameters" % self.rest_port, json=parameters)

            message = input_stream.receive()
            self.assertIsNone(message)

            requests.put("http://localhost:%d/start_pulse_id/%d" % (self.rest_port, start_pulse_id))

            message = input_stream.receive()
            self.assertIsNone(message)

            requests.put("http://localhost:%d/stop_pulse_id/%d" % (self.rest_port, stop_pulse_id))

            message = input_stream.receive()
            self.assertIsNotNone(message)

            self.assertTrue("data_api_request" in message.data.data)
            self.assertTrue("parameters" in message.data.data)

            received_data_api_request = json.loads(message.data.data["data_api_request"].value)
            received_parameters = json.loads(message.data.data["parameters"].value)

            self.assertDictEqual(received_parameters, parameters)
            self.assertEqual(received_data_api_request["range"]["startPulseId"], start_pulse_id)
            self.assertEqual(received_data_api_request["range"]["endPulseId"], stop_pulse_id)

            start_pulse_id = 1000
            stop_pulse_id = 1100
            parameters = {"general/created": "test2",
                          "general/user": "tester2",
                          "general/process": "test_process2",
                          "general/instrument": "mac2",
                          "output_file": "test2.h5"}

            message = input_stream.receive()
            self.assertIsNone(message)

            requests.post("http://localhost:%d/parameters" % self.rest_port, json=parameters)

            message = input_stream.receive()
            self.assertIsNone(message)

            requests.put("http://localhost:%d/start_pulse_id/%d" % (self.rest_port, start_pulse_id))

            message = input_stream.receive()
            self.assertIsNone(message)

            requests.put("http://localhost:%d/stop_pulse_id/%d" % (self.rest_port, stop_pulse_id))

            message = input_stream.receive()
            self.assertIsNotNone(message)

            self.assertTrue("data_api_request" in message.data.data)
            self.assertTrue("parameters" in message.data.data)

            received_data_api_request = json.loads(message.data.data["data_api_request"].value)
            received_parameters = json.loads(message.data.data["parameters"].value)

            self.assertDictEqual(received_parameters, parameters)
            self.assertEqual(received_data_api_request["range"]["startPulseId"], start_pulse_id)
            self.assertEqual(received_data_api_request["range"]["endPulseId"], stop_pulse_id)


