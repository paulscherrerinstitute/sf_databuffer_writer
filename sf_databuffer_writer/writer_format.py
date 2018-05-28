import logging
from time import time

import h5py
import numpy
from bsread.data.serialization import channel_type_deserializer_mapping
from bsread.writer import Writer

_logger = logging.getLogger(__name__)


DATA_DATASET_NAME = "data"


class BsreadH5Writer(object):
    def __init__(self, output_file, parameters):
        self.output_file = output_file
        self.parameters = parameters

        self.h5_writer = Writer()
        self.h5_writer.open_file(self.output_file)

        self.cached_channel_definitions = None
        self.first_iteration = True
        self.data_header_hash = None

    def prune_and_close(self, stop_pulse_id):
        start_time = time()
        _logger.info("Starting to close the file.")

        self.close()
        _logger.info("File closed in %s seconds.", time() - start_time)

    def _verify_datasets(self, message_data):

        # Data header is present in the message only when it has changed (or first message).
        if "data_header" not in message_data:
            raise ValueError("Data header not present in message: %s", message_data)

        data_header = message_data['data_header']
        data_header_hash = message_data["header"]["hash"]
        data_values = message_data["data"]
        n_channels = len(data_header['channels'])

        if self.first_iteration:
            self._prepare_format_datasets()

            n_channels = len(data_header['channels'])
            self.cached_channel_definitions = [None] * n_channels

            self.first_iteration = False

        if n_channels != len(self.cached_channel_definitions):
            raise ValueError("Number of channels in the stream changed."
                             "\nOriginal channels: %s\nNew data header: %s" %
                             (self.cached_channel_definitions, data_header))

        # Nothing to do if the data header is the same.
        if self.data_header_hash == message_data["header"]["hash"]:
            return

        _logger.info("Data header change detected.")
        self.data_header_hash = data_header_hash

        # Interpret the data header and add required datasets
        for channel_index, channel_definition in enumerate(data_header['channels']):

            channel_name = channel_definition['name']
            channel_value = data_values[channel_index]

            channel_group_name = '/data/' + channel_name + "/"

            # New channel.
            if self.cached_channel_definitions[channel_index] is None:

                _logger.debug("Creating datasets for channel_name '%s' at index %d.", channel_name, channel_index)

                self.h5_writer.add_dataset(channel_group_name + 'pulse_id', dataset_group_name='pulse_id', dtype='i8')
                self.h5_writer.add_dataset(channel_group_name + 'is_data_present', dataset_group_name='is_data_present',
                                           dtype='u1')

                self._setup_channel_data_dataset(channel_group_name, channel_index, channel_definition, channel_value)

            # A channel change counts only when data is present - otherwise we are not sure if data header is correct.
            elif channel_value is not None and self.cached_channel_definitions[channel_index] != channel_definition:

                _logger.info("Channel definition changed for channel_name '%s'."
                             "\nOld definition: %s\nNew definition%s.",
                             channel_name, self.cached_channel_definitions[channel_index], channel_definition)

                self._modify_channel_data_dataset(channel_group_name, channel_index, channel_definition)

    def write_message(self, message):
        message_data = message.data

        self._verify_datasets(message_data)

        data = message_data['data']

        self.h5_writer.write(data, dataset_group_name='data')

        self.h5_writer.write(message_data['pulse_ids'], dataset_group_name='pulse_id')

        # Because some channels might not be decoded properly, we have to write if data is written in a specific cell.
        is_data_valid = [1 if data_point is not None else 0 for data_point in data]
        self.h5_writer.write(is_data_valid, dataset_group_name='is_data_present')

    def _prepare_format_datasets(self):

        _logger.info("Initializing format datasets.")

        self.h5_writer.file.create_dataset("/general/created",
                                           data=numpy.string_(self.parameters["general/created"]))

        self.h5_writer.file.create_dataset("/general/instrument",
                                           data=numpy.string_(self.parameters["general/instrument"]))

        self.h5_writer.file.create_dataset("/general/process",
                                           data=numpy.string_(self.parameters["general/process"]))

        self.h5_writer.file.create_dataset("/general/user",
                                           data=numpy.string_(self.parameters["general/user"]))

    def _get_channel_data_dataset_definition(self, channel_definition):

        type_name = channel_definition.get('type', "float64")
        shape = channel_definition.get('shape', [1])

        dataset_shape = [1] + shape[::-1]
        dataset_max_shape = [None] + shape[::-1]
        dataset_type = channel_type_deserializer_mapping[type_name][0]

        if type_name == "string":
            dataset_shape = [1]
            dataset_max_shape = [None]
            dataset_type = h5py.special_dtype(vlen=str)

        return dataset_type, dataset_shape, dataset_max_shape

    def _setup_channel_data_dataset(self, channel_group_name, channel_index, channel_definition, channel_value):

        # If we do not have a channel value we cannot be sure that the header is correct or just default.
        if channel_value is None:
            _logger.info("No data for channel_name '%s' was received. Creating dataset stub.",
                         channel_definition['name'])

            self.h5_writer.add_dataset_stub(dataset_group_name='data',
                                            dataset_name=channel_group_name + DATA_DATASET_NAME)

            self.cached_channel_definitions[channel_index] = {}

        else:
            dtype, dataset_shape, dataset_max_shape = self._get_channel_data_dataset_definition(channel_definition)

            self.h5_writer.add_dataset(channel_group_name + 'data', dataset_group_name='data', shape=dataset_shape,
                                       maxshape=dataset_max_shape, dtype=dtype)

            self.cached_channel_definitions[channel_index] = channel_definition

    def _modify_channel_data_dataset(self, channel_group_name, channel_index, channel_definition):

        dtype, dataset_shape, dataset_max_shape = self._get_channel_data_dataset_definition(channel_definition)

        self.h5_writer.replace_dataset(dataset_group_name="data",
                                       dataset_name=channel_group_name + DATA_DATASET_NAME,
                                       dtype=dtype,
                                       shape=dataset_shape,
                                       maxshape=dataset_max_shape)

        self.cached_channel_definitions[channel_index] = channel_definition

    def close(self):
        self.h5_writer.close_file()
