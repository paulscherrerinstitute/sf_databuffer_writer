import copy
import json
from copy import deepcopy
from logging import getLogger
from time import time

from datetime import datetime
import requests

from sf_databuffer_writer import config

_logger = getLogger(__name__)


def get_writer_request(channels, parameters, start_pulse_id, stop_pulse_id):
    data_api_request = {
        "channels": [{'name': ch, 'backend': config.IMAGE_BACKEND if ch.endswith(":FPICTURE") else config.DATA_BACKEND}
                     for ch in channels],
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


def get_separate_writer_requests(channels, parameters, start_pulse_id, stop_pulse_id):

    camera_channels = []
    bsread_channels = []

    for channel in channels:
        if channel.endswith(":FPICTURE"):
            camera_channels.append(channel)
        else:
            bsread_channels.append(channel)

    yield get_writer_request(bsread_channels, parameters, start_pulse_id, stop_pulse_id)

    if len(camera_channels) > 0: 
        new_parameters = copy.deepcopy(parameters)
        if new_parameters["output_file"] != "/dev/null":
            if new_parameters["output_file"][-3:] == ".h5":
                new_parameters["output_file"] = new_parameters["output_file"][:-3] + ".IMAGES.h5"
            else:
                new_parameters["output_file"] = new_parameters["output_file"][:-3] + ".IMAGES.h5"
        yield get_writer_request(camera_channels, new_parameters, start_pulse_id, stop_pulse_id)

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


def transform_range_from_pulse_id_to_timestamp(data_api_request):

    new_data_api_request = deepcopy(data_api_request)

    try:

        mapping_request = {'range': {'startPulseId': data_api_request["range"]["startPulseId"],
                                     'endPulseId': data_api_request["range"]["endPulseId"]+1}}

        mapping_response = requests.post(url=config.DATA_API_QUERY_ADDRESS + "/mapping", json=mapping_request).json()

        _logger.info("Response to mapping request: %s", mapping_response)

        del new_data_api_request["range"]["startPulseId"]
        new_data_api_request["range"]["startSeconds"] = mapping_response[0]["start"]["globalSeconds"]

        del new_data_api_request["range"]["endPulseId"]
        new_data_api_request["range"]["endSeconds"] = mapping_response[0]["end"]["globalSeconds"]

        _logger.info("Transformed request to startSeconds and endSeconds. %s" % new_data_api_request)

    except Exception as e:
        raise RuntimeError("Cannot retrieve the pulse_id to timestamp mapping.") from e

    return new_data_api_request


def get_timestamp_range_from_api_request(data_api_request, request_timestamp):
    if request_timestamp is None:
        raise ValueError("Request timestamp cannot be none. Do you have the latest sf_databuffer_writer version?")

    start_pulse_id = data_api_request["range"]["startPulseId"]
    stop_pulse_id = data_api_request["range"]["endPulseId"]

    acquisition_time_seconds = (stop_pulse_id - start_pulse_id) / 100
    start_delay_seconds = 10

    # Add second because we throw away milliseconds.
    request_timestamp += 1
    end_date = datetime.fromtimestamp(request_timestamp)
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S.000+02:00")

    # start_date = end_date - timedelta(seconds=acquisition_time_seconds + start_delay_seconds)
    start_date = request_timestamp - (acquisition_time_seconds + start_delay_seconds)
    start_date = datetime.fromtimestamp(start_date)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000+02:00")

    return start_date, end_date


def filter_unwanted_pulse_ids(json_data, start_pulse_id, stop_pulse_id):
    def get_index_from_pulse_id(name, data, target_pulse_id, direction=1):
        for index, pulse_id in ((i, d["pulseId"]) for i, d in enumerate(data[::direction])):
            if direction == 1 and pulse_id >= target_pulse_id:
                return index

            elif direction == -1 and pulse_id <= target_pulse_id:
                return len(data) - index - 1

        raise ValueError("Pulse id %s not found in channel %s." % (target_pulse_id, name))

    start_time = time()
    for channel_data in json_data:
        try:

            channel_name = channel_data["channel"]["name"]
            data = channel_data["data"]

            start_index = get_index_from_pulse_id(channel_name, data, start_pulse_id)
            stop_index = get_index_from_pulse_id(channel_name, data, stop_pulse_id, direction=-1)

            data[:] = data[start_index:stop_index + 1]

        except Exception as e:
            _logger.error("Data filtering could not be done. Exception: ", e)

    _logger.info("Filtering pulse_ids took %s seconds." % (time() - start_time))
