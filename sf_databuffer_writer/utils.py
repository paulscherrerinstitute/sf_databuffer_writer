import json
from logging import getLogger
from time import time

from sf_databuffer_writer import config

_logger = getLogger(__name__)


def get_writer_request(channels, parameters, start_pulse_id, stop_pulse_id):

    data_api_request = {
        "channels": [{'name': ch, 'backend': "sf-databuffer"} for ch in channels],
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


def verify_channels(input_channels):

    _logger.info("Verifying limit of max %d bsread channels." % config.BROKER_CHANNELS_LIMIT)

    channels = [x for x in input_channels if x]
    n_channels = len(channels)
    if n_channels > config.BROKER_CHANNELS_LIMIT:
        raise ValueError("Too many bsread channels. configured/limit: %d/%d."
                         % (n_channels, config.BROKER_CHANNELS_LIMIT))

    _logger.info("Verifying limit of max %d bsread picture channels." % config.BROKER_CHANNELS_LIMIT_PICTURE)

    picture_channels = [x for x in channels if x.endswith(":FPICTURE")]
    n_picture_channels = len(picture_channels)
    if n_picture_channels > config.BROKER_CHANNELS_LIMIT_PICTURE:
        raise ValueError("Too many picture channels. configured/limit: %d/%d."
                         % (n_picture_channels, config.BROKER_CHANNELS_LIMIT_PICTURE))
