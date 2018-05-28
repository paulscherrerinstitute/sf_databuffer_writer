import argparse
import logging

import bottle

from sf_databuffer_writer import config
from sf_databuffer_writer.broker_manager import BrokerManager
from sf_databuffer_writer.rest_api import register_rest_interface

_logger = logging.getLogger(__name__)


def start_server(channels, output_port, queue_length, rest_port):
    _logger.info("Writing data for channels: %s", channels)
    _logger.debug("Setting queue length to %s.", queue_length)

    app = bottle.Bottle()
    manager = BrokerManager(channels, output_port, queue_length)
    register_rest_interface(app, manager)

    _logger.info("Broker started.")

    try:
        _logger.info("Starting rest API on port %s." % rest_port)
        bottle.run(app=app, host="127.0.0.1", port=rest_port)
    finally:
        pass


def run():
    parser = argparse.ArgumentParser(description='bsread data buffer broker')

    parser.add_argument("-c", "--channels_file", help="JSON file with channels to buffer.")

    parser.add_argument('-o', '--output_port', default=config.DEFAULT_STREAM_OUTPUT_PORT,
                        help="Port to bind the output stream to.")
    parser.add_argument("-q", "--queue_length", default=config.DEFAULT_QUEUE_LENGTH,
                        help="Length of the zmq queue.")

    parser.add_argument("--rest_port", type=int, help="Port for REST api.", default=config.DEFAULT_BROKER_REST_PORT)

    parser.add_argument("--log_level", default=config.DEFAULT_LOG_LEVEL,
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    _logger.info("Loading channels list file '%s'.", arguments.channels_file)

    with open(arguments.channels_file) as input_file:
        file_lines = input_file.readlines()
        channels = [channel.strip() for channel in file_lines if not channel.strip().startswith("#") and channel.strip()]

    start_server(channels=channels,
                 output_port=arguments.output_port,
                 queue_length=arguments.queue_length,
                 rest_port=arguments.rest_port)


if __name__ == "__main__":
    run()
