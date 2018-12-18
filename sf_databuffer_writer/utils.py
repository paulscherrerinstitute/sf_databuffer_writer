import json
from logging import getLogger
from time import time

_logger = getLogger(__name__)


def get_writer_request(channels, parameters, start_pulse_id, stop_pulse_id):

    data_api_request = {
        "channels": [{'name': ch} for ch in channels],
        "range": {
            "startPulseId": start_pulse_id,
            "endPulseId": stop_pulse_id},
        "response": {
            "format": "json",
            "compression": "none"},
        "eventFields": ["channel", "pulseId", "value", "shape", "globalDate"],
        "configFields": ["type", "shape"]
    }

    write_request = {
        "data_api_request": json.dumps(data_api_request),
        "parameters": json.dumps(parameters),
        "timestamp": time()
    }

    return write_request

