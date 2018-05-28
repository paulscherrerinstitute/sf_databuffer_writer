import json
import unittest

import os

from sf_databuffer_writer.broker_manager import BrokerManager


class MockRequestSender(object):
    def send(self, write_request):
        self.write_request = write_request


class TestBrokerManager(unittest.TestCase):
    TEST_AUDIT_FILE = "ignore_output.txt"

    def tearDown(self):
        try:
            os.remove(TestBrokerManager.TEST_AUDIT_FILE)
        except:
            pass

    def test_write_request(self):

        request_sender = MockRequestSender()
        channels = ["test_1", "test_2"]
        start_pulse_id = 100
        stop_pulse_id = 120
        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": "test.h5"}

        manager = BrokerManager(request_sender, channels, TestBrokerManager.TEST_AUDIT_FILE)

        manager.set_parameters(parameters)
        manager.start_writer(start_pulse_id)
        manager.stop_writer(stop_pulse_id)

        data_api_request = json.loads(request_sender.write_request["data_api_request"])
        request_parameters = json.loads(request_sender.write_request["parameters"])

        self.assertEqual(data_api_request["range"]["startPulseId"], start_pulse_id)
        self.assertEqual(data_api_request["range"]["endPulseId"], stop_pulse_id)
        self.assertListEqual(data_api_request["channels"], channels)

        self.assertDictEqual(request_parameters, parameters)

        self.assertEqual(data_api_request["mapping"]["incomplete"], "fill-null",
                         "fill-null strategy is needed for writer.")

        self.assertListEqual(data_api_request["eventFields"], ["channel", "pulseId", "value", "shape"])
        self.assertListEqual(data_api_request["configFields"], ["type", "shape"])

        start_pulse_id = 1000
        stop_pulse_id = 1100
        parameters = {"general/created": "test2",
                      "general/user": "tester2",
                      "general/process": "test_process2",
                      "general/instrument": "mac2",
                      "output_file": "test2.h5"}

        manager.set_parameters(parameters)
        manager.start_writer(start_pulse_id)
        manager.stop_writer(stop_pulse_id)

        data_api_request = json.loads(request_sender.write_request["data_api_request"])
        request_parameters = json.loads(request_sender.write_request["parameters"])

        self.assertEqual(data_api_request["range"]["startPulseId"], start_pulse_id)
        self.assertEqual(data_api_request["range"]["endPulseId"], stop_pulse_id)

        self.assertDictEqual(request_parameters, parameters)

    def test_audit_file(self):
        request_sender = MockRequestSender()
        channels = ["test_1", "test_2"]
        start_pulse_id = 100
        stop_pulse_id = 120
        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": "test.h5"}

        manager = BrokerManager(request_sender, channels, TestBrokerManager.TEST_AUDIT_FILE)

        manager.set_parameters(parameters)
        manager.start_writer(start_pulse_id)
        manager.stop_writer(stop_pulse_id)

        data_api_request = json.loads(request_sender.write_request["data_api_request"])
        request_parameters = json.loads(request_sender.write_request["parameters"])

        with open(TestBrokerManager.TEST_AUDIT_FILE) as input_file:
            lines = input_file.readlines()

        audit_log = json.loads(lines[0][18:])
        audit_data_api_request = json.loads(audit_log["data_api_request"])
        audit_request_parameters = json.loads(audit_log["parameters"])

        self.assertDictEqual(data_api_request, audit_data_api_request)
        self.assertDictEqual(request_parameters, audit_request_parameters)

    def test_manager_status(self):
        request_sender = MockRequestSender()
        channels = []

        manager = BrokerManager(request_sender, channels, TestBrokerManager.TEST_AUDIT_FILE)

        self.assertEqual(manager.get_status(), "waiting")

        manager.start_writer(100)

        self.assertEqual(manager.get_status(), "writing")

        manager.stop_writer(120)

        self.assertEqual(manager.get_status(), "waiting")
