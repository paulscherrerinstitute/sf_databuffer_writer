import unittest
from multiprocessing import Process, Manager
from time import sleep

import bottle
import json
from bsread import PUB

from sf_databuffer_writer import config
from sf_databuffer_writer.broker_manager import StreamRequestSender
from sf_databuffer_writer.utils import get_writer_request


class TestStreamRequestSender(unittest.TestCase):

    def setUp(self):
        self.manager = Manager()
        self.data_container = self.manager.dict()

        def http_server(data):

            app = bottle.Bottle()

            @app.put("/notify")
            def print_request():
                data["request"] = bottle.request.json

            bottle.run(app=app, host="localhost", port=10200)

        self.http_process = Process(target=http_server, args=(self.data_container,))
        self.http_process.start()
        sleep(0.5)

    def tearDown(self):
        self.http_process.terminate()

    def test_write_request(self):

        request_sender = StreamRequestSender(12000, 100, 100, PUB, config.DEFAULT_EPICS_WRITER_URL)

        channels = ["channel_1", "channel_2", "channel_3:FPICTURE"]

        start_pulse_id = 100
        stop_pulse_id = 120

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": "test.h5"}

        writer_request = get_writer_request(channels, parameters, start_pulse_id, stop_pulse_id)

        request_sender.send(writer_request)

        sleep(0.5)

        self.assertTrue("request" in self.data_container)
        data = self.data_container["request"]

        self.assertTrue("range" in data)
        self.assertTrue("startPulseId" in data["range"])
        self.assertTrue("endPulseId" in data["range"])

        self.assertTrue("parameters" in data)
        self.assertTrue("output_file" in data["parameters"])

        self.assertTrue("data_api_request" in writer_request)
        data_api_request = json.loads(writer_request["data_api_request"])

        self.assertTrue("channels" in data_api_request)
        for channel_name, backend in ((x["name"], x["backend"])
                                      for x in data_api_request["channels"]):

            if channel_name.endswith(":FPICTURE"):
                self.assertEqual(backend, config.IMAGE_BACKEND)
            else:
                self.assertEqual(backend, config.DATA_BACKEND)

        self.assertListEqual(channels, [x["name"] for x in data_api_request["channels"]])

    def test_write_request_with_channels(self):
        channels = ["channel_1", "channel_2", "channel_3:FPICTURE"]
        override_channels = ["override"]

        parameters = {"general/created": "test",
                      "general/user": "tester",
                      "general/process": "test_process",
                      "general/instrument": "mac",
                      "output_file": "test.h5",
                      "channels": ["override"]}

        writer_request = get_writer_request(channels, parameters, 0, 100)

        self.assertTrue("data_api_request" in writer_request)
        data_api_request = json.loads(writer_request["data_api_request"])

        self.assertListEqual(override_channels, [x["name"] for x in data_api_request["channels"]])




