from bsread import PULL
import logging

from bsread.sender import Sender

_logger = logging.getLogger(__name__)


def audit_write_request(write_request):
    print(write_request)


class BrokerManager(object):
    REQUIRED_PARAMETERS = ["general/created", "general/user", "general/process", "general/instrument"]

    def __init__(self, channels, output_port, queue_length, send_timeout=1000, mode=PULL):

        self.channels = channels
        _logger.info("Starting broker manager with channels %s.", self.channels)

        self.current_parameters = None
        self.current_start_pulse_id = None

        self.request_sender = StreamRequestSender(output_port=output_port,
                                                  queue_length=queue_length,
                                                  send_timeout=send_timeout,
                                                  mode=mode)

    def set_parameters(self, parameters):

        _logger.debug("Setting parameters %s." % parameters)

        if not all(x in parameters for x in self.REQUIRED_PARAMETERS):
            raise ValueError("Missing mandatory parameters. Mandatory parameters '%s' but received '%s'." %
                             (self.REQUIRED_PARAMETERS, list(parameters.keys())))

        self.current_parameters = parameters

    def get_parameters(self):
        return self.current_parameters

    def get_status(self):

        if self.current_parameters is None and self.current_start_pulse_id is None:
            return "stopped"

        if self.current_start_pulse_id is None:
            return "waiting"

        if self.current_start_pulse_id is not None:
            return "writing"

        return "error"

    def stop(self):
        _logger.info("Stopping bsread broker session.")

        self.current_parameters = None
        self.current_start_pulse_id = None

    def start_writer(self, start_pulse_id):

        _logger.info("Set start_pulse_id %d." % start_pulse_id)
        self.current_start_pulse_id = start_pulse_id

    def stop_writer(self, stop_pulse_id):
        _logger.info("Set stop_pulse_id=%d", stop_pulse_id)

        data_api_request = {
            "channels": self.channels,
            "range": {
                "startPulseId": self.current_start_pulse_id,
                "endPulseId": self.stop_pulse_id},
            "response": {
                "format": "json",
                "compression": "none"},
            "mapping": {
                "incomplete": "fill-null"
            }
        }

        write_request = {
            "data_api_request": data_api_request,
            "parameters": self.current_parameters
        }

        self.current_start_pulse_id = None
        self.current_parameters = None

        audit_write_request(write_request)
        self.request_sender.send(write_request)

    def get_statistics(self):
        return {"start_pulse_id": self.start_pulse_id,
                "stop_pulse_id": self.stop_pulse_id,
                "last_pulse_id": self.last_pulse_id}


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
        self.output_stream.send(data={"write_request": write_request})
