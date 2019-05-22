import argparse
import requests
import logging

_logger = logging.getLogger(__name__)


def create_broker_request(channels, output_file, start_pulse_id, stop_pulse_id, rest_api_host, compact):

    _logger.info("Broker hostname: %s", rest_api_host)

    parameters = {"general/created": "just now",
                  "general/user": "root",
                  "general/process": "manual_broker_test.py",
                  "general/instrument": "bsread",
                  "output_file": output_file,
                  "output_file_format": "compact" if compact else None}

    _logger.info("Sending request with parameters: %s", parameters)
    requests.post("%s/parameters" % rest_api_host, json=parameters)

    _logger.info("Sending start_pulse_id: %d", start_pulse_id)
    requests.put("%s/start_pulse_id/%d" % (rest_api_host, start_pulse_id))

    _logger.info("Sending stop_pulse_id: %d", stop_pulse_id)
    requests.put("%s/stop_pulse_id/%d" % (rest_api_host, stop_pulse_id))
    
    _logger.info("Request sent. Soon check for the output file %s.", output_file)


def run():
    parser = argparse.ArgumentParser(description='Manual broker test')
    parser.add_argument("channels_file", type=str, help="JSON file with channels to buffer.")
    parser.add_argument("output_file", type=str, help="Where to write the output file.")
    parser.add_argument("start_pulse_id", type=int, help="Start pulse_id to write.")
    parser.add_argument("stop_pulse_id", type=int, help="Stop pulse_id to write.")
    parser.add_argument("--host", help="Hostname of broker rest API\nFormat: hostname:port", default="localhost:1002")
    parser.add_argument("--compact", action='store_true', help="broker rest API\nFormat: hostname:port",
                        default="localhost:1002")
    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], help="Log level to use.")

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format='[%(levelname)s] %(message)s')

    with open(args.channels_file) as input_file:
        file_lines = input_file.readlines()
        channels = [channel.strip() for channel in file_lines
            if not channel.strip().startswith("#") and channel.strip()]

    create_broker_request(
        channels=channels,
        output_file=args.output_file,
        start_pulse_id=args.start_pulse_id,
        stop_pulse_id=args.stop_pulse_id,
        rest_api_host="http://%s" % args.host,
        compact=args.compact
    )


if __name__ == "__main__":
    run()
