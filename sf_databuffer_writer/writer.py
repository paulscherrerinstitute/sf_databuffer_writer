import argparse
import logging
import os

from bsread import source

_logger = logging.getLogger(__name__)


def create_folders(output_file):

    filename_folder = os.path.dirname(output_file)

    # Create a folder if it does not exist.
    if filename_folder and not os.path.exists(filename_folder):
        _logger.info("Creating folder '%s'.", filename_folder)
        os.makedirs(filename_folder, exist_ok=True)
    else:
        _logger.info("Folder '%s' already exists.", filename_folder)


def process_requests(stream_address):
    pass


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
