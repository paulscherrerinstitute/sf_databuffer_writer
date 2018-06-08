import argparse
import logging
import os
import json
from datetime import datetime
from time import time, sleep

import requests
from bsread import source, PULL

from sf_databuffer_writer import config
from sf_databuffer_writer.writer_format import DataBufferH5Writer

_logger = logging.getLogger(__name__)


def create_folders(output_file):

    filename_folder = os.path.dirname(output_file)

    # Create a folder if it does not exist.
    if filename_folder and not os.path.exists(filename_folder):
        _logger.info("Creating folder '%s'.", filename_folder)
        os.makedirs(filename_folder, exist_ok=True)
    else:
        _logger.info("Folder '%s' already exists.", filename_folder)


def audit_failed_write_request(data_api_request, parameters):

    filename = None

    write_request = {
        "data_api_request": json.dumps(data_api_request),
        "parameters": json.dumps(parameters)
    }

    try:
        filename = parameters["output_file"] + ".err"

        current_time = datetime.now().strftime(config.AUDIT_FILE_TIME_FORMAT)

        with open(filename, "w") as audit_file:
            audit_file.write("[%s] %s" % (current_time, json.dumps(write_request)))

    except Exception as e:
        _logger.error("Error while trying to write request %s to file %s." % (write_request, filename), e)


def write_data_to_file(parameters, json_data):
    output_file = parameters["output_file"]

    _logger.info("Writing data to output_file: %s", output_file)

    if not json_data:
        raise ValueError("Received data from data_api is empty. json_data=%s" % json_data)

    if not parameters:
        raise ValueError("Received parameters from broker are empty. parameters=%s" % parameters)

    writer = DataBufferH5Writer(output_file, parameters)
    writer.write_data(json_data)
    writer.close()


def get_data_from_buffer(data_api_request):

    _logger.info("Loading data from from startPulseId=%s to endPulseId=%s.",
                 data_api_request["range"]["startPulseId"], data_api_request["range"]["endPulseId"])

    _logger.debug("Data API request: %s", data_api_request)
    response = requests.post(url=config.DATA_API_QUERY_ADDRESS, json=data_api_request)

    if not response.ok:
        raise RuntimeError("Error while trying to get data from the dispatching layer.", response.text)

    return response.json()


def process_requests(stream_address, receive_timeout=None, mode=PULL, data_retrieval_delay=None):

    if receive_timeout is None:
        receive_timeout = config.DEFAULT_RECEIVE_TIMEOUT

    if data_retrieval_delay is None:
        data_retrieval_delay = config.DEFAULT_DATA_RETRIEVAL_DELAY

    source_host, source_port = stream_address.rsplit(":", maxsplit=1)

    source_host = source_host.split("//")[1]
    source_port = int(source_port)

    _logger.info("Connecting to broker host %s:%s." % (source_host, source_port))
    _logger.info("Using data_retrieval_delay=%s seconds." % data_retrieval_delay)

    with source(host=source_host, port=source_port, mode=mode, receive_timeout=receive_timeout) as input_stream:

        while True:

            data_api_request = None
            parameters = None

            try:
                message = input_stream.receive()

                if message is None:
                    continue

                data_api_request = json.loads(message.data.data["data_api_request"].value)
                parameters = json.loads(message.data.data["parameters"].value)

                _logger.info("Received request to write file %s from startPulseId=%s to endPulseId=%s" % (
                             parameters["output_file"],
                             data_api_request["range"]["startPulseId"],
                             data_api_request["range"]["endPulseId"]))

                request_timestamp = message.data.data["timestamp"]
                current_timestamp = time()
                # sleep time = target sleep time - time that has already passed.
                adjusted_retrieval_delay = data_retrieval_delay - (current_timestamp - request_timestamp)

                if adjusted_retrieval_delay < 0:
                    adjusted_retrieval_delay = 0

                _logger.info("Request timestamp=%s, current_timestamp=%s, adjusted_retrieval_delay=%s." %
                             (request_timestamp, current_timestamp, adjusted_retrieval_delay))

                _logger.info("Sleeping for %s seconds before calling the data api." % adjusted_retrieval_delay)
                sleep(adjusted_retrieval_delay)
                _logger.info("Sleeping finished. Retrieving data.")

                start_time = time()
                data = get_data_from_buffer(data_api_request)
                _logger.info("Data retrieval took %s seconds." % (time() - start_time))

                start_time = time()
                write_data_to_file(parameters, data)
                _logger.info("Data writing took %s seconds." % (time() - start_time))

            except Exception as e:
                audit_failed_write_request(data_api_request, parameters)

                _logger.error("Error while trying to write a requested data range.", e)


def start_server(stream_address, user_id=-1, data_retrieval_delay=None):

    if user_id != -1:
        _logger.info("Setting bsread writer uid and gid to %s.", user_id)
        os.setgid(user_id)
        os.setuid(user_id)

    else:
        _logger.info("Not changing process uid and gid.")

    process_requests(stream_address, data_retrieval_delay=data_retrieval_delay)


def run():
    parser = argparse.ArgumentParser(description='bsread data buffer writer')

    parser.add_argument("stream_address", help="Address of the stream to connect to.")
    parser.add_argument("user_id", type=int, help="user_id under which to run the writer process."
                                                  "Use -1 for current user.")
    parser.add_argument("--data_retrieval_delay", default=config.DEFAULT_DATA_RETRIEVAL_DELAY, type=int,
                        help="Time to wait before asking the data-api for the data.")

    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    start_server(stream_address=arguments.stream_address,
                 user_id=arguments.user_id,
                 data_retrieval_delay=arguments.data_retrieval_delay)


if __name__ == "__main__":
    run()
