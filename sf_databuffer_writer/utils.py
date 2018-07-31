from datetime import datetime, timedelta
from logging import getLogger
from time import time

_logger = getLogger(time())


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

    start_date = end_date - timedelta(seconds=acquisition_time_seconds + start_delay_seconds)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.000+02:00")

    return start_date, end_date


def filter_unwanted_pulse_ids(data, start_pulse_id, stop_pulse_id):
    try:

        def get_index_from_pulse_id(name, data, target_pulse_id, direction=1):
            for index, pulse_id in (i, d["pulseId"] for i, d in enumerate(data[::direction])):
                if direction == 1 and pulse_id >= target_pulse_id:
                    return index
                elif direction == -1 and pulse_id <= target_pulse_id:
                    return index

            raise ValueError("Pulse id %s not found in channel %s." % (pulse_id, name))

        start_time = time()
        for channel_data in data:

            channel_name = channel_data["channel"]["name"]
            data = channel_data["data"]

            start_index = get_index_from_pulse_id(channel_name, data, start_pulse_id)
            stop_index = get_index_from_pulse_id(channel_name, data, stop_pulse_id, direction=-1)

            data[:] = data[start_index:stop_index+1]

        _logger.info("Filtering pulse_ids took %s seconds." % (time()-start_time))

    except:
        pass