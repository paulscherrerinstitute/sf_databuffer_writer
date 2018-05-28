from datetime import datetime

import logging
import json
from bsread.sender import Sender

from sf_databuffer_writer import config

_logger = logging.getLogger(__name__)


def audit_write_request(filename, write_request):

    try:
        current_time = datetime.now().strftime(config.AUDIT_FILE_TIME_FORMAT)

        with open(filename, mode="a") as audit_file:
            audit_file.write("[%s] %s" % (current_time, json.dumps(write_request)))

    except Exception as e:
        _logger.error("Error while trying to append request %s to file %s.", write_request, filename, e)


class BrokerManager(object):
    REQUIRED_PARAMETERS = ["general/created", "general/user", "general/process", "general/instrument", "output_file"]

    def __init__(self, request_sender, channels, audit_filename=None):

        if audit_filename is None:
            audit_filename = config.DEFAULT_AUDIT_FILENAME
        self.audit_filename = audit_filename
        _logger.info("Writing requests audit log to file %s.", self.audit_filename)

        self.channels = channels
        _logger.info("Starting broker manager with channels %s.", self.channels)

        self.current_parameters = None
        self.current_start_pulse_id = None

        self.request_sender = request_sender

        self.statistics = {"n_processed_requests": 0,
                           "process_startup_time": datetime.now().strftime(config.AUDIT_FILE_TIME_FORMAT)}

    def set_parameters(self, parameters):

        _logger.debug("Setting parameters %s." % parameters)

        if not all(x in parameters for x in self.REQUIRED_PARAMETERS):
            raise ValueError("Missing mandatory parameters. Mandatory parameters '%s' but received '%s'." %
                             (self.REQUIRED_PARAMETERS, list(parameters.keys())))

        self.current_parameters = parameters

    def get_parameters(self):
        return self.current_parameters

    def get_status(self):

        if self.current_start_pulse_id is not None:
            return "writing"

        return "waiting"

    def stop(self):
        _logger.info("Stopping bsread broker session.")

        self.current_parameters = None
        self.current_start_pulse_id = None

    def start_writer(self, start_pulse_id):

        if self.current_start_pulse_id is not None:
            _logger.warning("Previous acquisition was still running. The previous run will not be processed.")

            _logger.warning({"current_parameters": self.current_parameters,
                             "current_start_pulse_id": self.current_start_pulse_id,
                             "new_start_pulse_id": start_pulse_id})

        _logger.info("Set start_pulse_id %d." % start_pulse_id)
        self.current_start_pulse_id = start_pulse_id

    def stop_writer(self, stop_pulse_id):
        _logger.info("Set stop_pulse_id=%d", stop_pulse_id)

        data_api_request = {
            "channels": self.channels,
            "range": {
                "startPulseId": self.current_start_pulse_id,
                "endPulseId": stop_pulse_id},
            "response": {
                "format": "json",
                "compression": "none"},
            "mapping": {
                "incomplete": "fill-null"
            },
            "eventFields": ["channel", "pulseId", "value", "shape"],
            "configFields": ["type", "shape"]
        }

        write_request = {
            "data_api_request": json.dumps(data_api_request),
            "parameters": json.dumps(self.current_parameters)
        }

        self.current_start_pulse_id = None
        self.current_parameters = None

        audit_write_request(self.audit_filename, write_request)

        self.request_sender.send(write_request)

        self.statistics["last_sent_write_request"] = write_request
        self.statistics["last_sent_write_request_time"] = datetime.now().strftime(config.AUDIT_FILE_TIME_FORMAT)
        self.statistics["n_processed_requests"] += 1

    def get_statistics(self):
        return self.statistics


class StreamRequestSender(object):
    def __init__(self, output_port, queue_length, send_timeout, mode):
        self.output_port = output_port
        self.queue_length = queue_length
        self.send_timeout = send_timeout
        self.mode = mode

        _logger.info("Starting stream request sender with output_port=%s, queue_length=%s, send_timeout=%s and mode=%s",
                     self.output_port, self.queue_length, self.send_timeout, self.mode)

        self.output_stream = Sender(port=self.output_port,
                                    queue_size=self.queue_length,
                                    send_timeout=self.send_timeout,
                                    mode=self.mode)

        self.output_stream.open()

    def send(self, write_request):
        _logger.info("Sending write write_request: %s", write_request)
        self.output_stream.send(data=write_request)
