import json
import unittest

from sf_databuffer_writer.utils import get_separate_writer_requests


class TestUtils(unittest.TestCase):

    def test_get_separate_writer_requests(self):
        start_pulse_id = 0
        stop_pulse_id = 10
        parameters = {"output_file": "test.h5"}

        channels = ["channel_1", "channel_2"]
        write_requests = list(get_separate_writer_requests(channels, parameters, start_pulse_id, stop_pulse_id))
        self.assertEqual(len(write_requests), 1, "All non-camera channels should be bunched together.")

        channels = ["channel_1", "camera:FPICTURE", "channel_2"]
        write_requests = list(get_separate_writer_requests(channels, parameters, start_pulse_id, stop_pulse_id))
        self.assertEqual(len(write_requests), 2, "The camera channel should be separate from the non-camera ones.")

        channels = ["channel_1", "camera_1:FPICTURE", "channel_2", "camera_2:FPICTURE"]
        write_requests = list(get_separate_writer_requests(channels, parameters, start_pulse_id, stop_pulse_id))
        self.assertEqual(len(write_requests), 3, "Camera channels are one per request, bsread is bunched in one.")

        bsread_channels_found = False
        for write_request in write_requests:
            channels = json.loads(write_request["data_api_request"])["channels"]

            if len(channels) > 1:
                self.assertFalse(bsread_channels_found, "There should be only 1 request with more than 1 channel.")
                bsread_channels_found = True

                for channel in channels:
                    self.assertFalse(channel["name"].endswith(":FPICTURE"), "There should be no image channels here.")

            else:
                self.assertTrue(channels[0]["name"].endswith(":FPICTURE"), "This channel should be a camera channel.")
                self.assertEqual(json.loads(write_request["parameters"])["output_file"],
                                 parameters["output_file"] + "_" + channels[0]["name"][:-9] + ".h5")

        self.assertTrue(bsread_channels_found)
