import argparse
import logging

import bottle
from bsread import PUSH

from sf_databuffer_writer import config
from sf_databuffer_writer.broker_manager import BrokerManager, StreamRequestSender
from sf_databuffer_writer.rest_api import register_rest_interface

_logger = logging.getLogger(__name__)


def start_server(channels, output_port, queue_length, rest_port, audit_trail_only=False, epics_writer_url=None):
    _logger.info("Writing data for channels: %s", channels)
    _logger.debug("Setting queue length to %s.", queue_length)

    app = bottle.Bottle()

    request_sender = StreamRequestSender(output_port=output_port,
                                         queue_length=queue_length,
                                         send_timeout=config.DEFAULT_SEND_TIMEOUT,
                                         mode=PUSH,
                                         epics_writer_url=epics_writer_url)

    manager = BrokerManager(request_sender=request_sender,
                            channels=channels,
                            audit_trail_only=audit_trail_only)

    register_rest_interface(app, manager)

    _logger.info("Broker started.")

    try:
        _logger.info("Starting rest API on port %s." % rest_port)
        bottle.run(app=app, host="127.0.0.1", port=rest_port)
    finally:
        pass


def run():
    parser = argparse.ArgumentParser(description='bsread broker')

    parser.add_argument("-c", "--channels_file", help="JSON file with channels to buffer.")

    parser.add_argument('-o', '--output_port', type=int, default=config.DEFAULT_STREAM_OUTPUT_PORT,
                        help="Port to bind the output stream to.")
    parser.add_argument("-q", "--queue_length", type=int, default=config.DEFAULT_QUEUE_LENGTH,
                        help="Length of the zmq queue.")

    parser.add_argument("--rest_port", type=int, help="Port for REST api.", default=config.DEFAULT_BROKER_REST_PORT)

    parser.add_argument("--log_level", default=config.DEFAULT_LOG_LEVEL,
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    parser.add_argument("--audit_trail_only", action="store_true",
                        help="Do not send data over ZMQ. Write audit trail only.")

    parser.add_argument("--epics_writer_url", default=None,
                        help="Epics writer URL to notify for new acquisition.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    _logger.info("Loading channels list file '%s'.", arguments.channels_file)

    with open(arguments.channels_file) as input_file:
        file_lines = input_file.readlines()
        channels = [channel.strip() for channel in file_lines
                    if not channel.strip().startswith("#") and channel.strip()]

    start_server(channels=channels,
                 output_port=arguments.output_port,
                 queue_length=arguments.queue_length,
                 rest_port=arguments.rest_port,
                 audit_trail_only=arguments.audit_trail_only,
                 epics_writer_url=arguments.epics_writer_url
                 )


if __name__ == "__main__":
    run()
