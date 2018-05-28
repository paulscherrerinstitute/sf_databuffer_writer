import argparse
import logging
import os
import json
from time import time

import datetime
import requests
from bsread import source, PULL

from sf_databuffer_writer import config

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

        with open(filename) as audit_file:
            audit_file.write("[%s] %s" % (current_time, json.dumps(write_request)))

    except Exception as e:
        _logger.error("Error while trying to write request %s to file %s", write_request, filename)


def write_data_to_file(parameters, data):
    # TODO: Write the data file in correct format.
    pass


def get_data_from_buffer(data_api_request):

    _logger.info("Loading data from from startPulseId=%s to endPulseId=%s.",
                 data_api_request["range"]["startPulseId"], data_api_request["range"]["endPulseId"])

    _logger.debug("Data API request: %s", data_api_request)
    response = requests.post(url=config.DATA_API_QUERY_ADDRESS, json=data_api_request)

    if not response.ok:
        raise RuntimeError("Error while trying to get data from the dispatching layer.", response.text)

    return response.json()


def process_requests(stream_address, receive_timeout=None, mode=PULL):

    if receive_timeout is None:
        receive_timeout = config.DEFAULT_RECEIVE_TIMEOUT

    source_host, source_port = stream_address.rsplit(":", maxsplit=1)

    source_host = source_host.split("//")[1]
    source_port = int(source_port)

    _logger.info("Connecting to broker host %s:%s.", source_host, source_port)

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

                _logger.info("Received request to write file %s from startPulseId=%s to endPulseId=%s",
                             parameters["output_file"],
                             data_api_request["range"]["startPulseId"],
                             data_api_request["range"]["endPulseId"])

                start_time = time()
                data = get_data_from_buffer(data_api_request)
                _logger.info("Data retrieval took %s seconds.", time() - start_time)

                start_time = time()
                write_data_to_file(parameters, data)
                _logger.info("Data writing took %s seconds.", time() - start_time)

            except Exception as e:
                audit_failed_write_request(data_api_request, parameters)

                _logger.error("Error while trying to write a requested data range.", e)


def start_server(stream_address, user_id):

    if user_id != -1:
        _logger.info("Setting bsread writer uid and gid to %s.", user_id)
        os.setgid(user_id)
        os.setuid(user_id)

    else:
        _logger.info("Not changing process uid and gid.")

    process_requests(stream_address)


def run():
    parser = argparse.ArgumentParser(description='bsread data buffer writer')

    parser.add_argument("stream_address", help="Address of the stream to connect to.")
    parser.add_argument("user_id", type=int, help="user_id under which to run the writer process."
                                                  "Use -1 for current user.")

    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    start_server(stream_address=arguments.stream_address,
                 user_id=arguments.user_id)


if __name__ == "__main__":
    run()
