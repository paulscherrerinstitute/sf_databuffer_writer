import logging

import h5py
import numpy
from bsread.data.serialization import channel_type_deserializer_mapping
from bsread.writer import Writer

_logger = logging.getLogger(__name__)


DATA_DATASET_NAME = "data"


class DataBufferH5Writer(object):
    def __init__(self, output_file, parameters):
        self.output_file = output_file
        self.parameters = parameters

        self.h5_writer = Writer()
        self.h5_writer.open_file(self.output_file)

    def _create_datasets(self, datasets_definition):

        self._prepare_format_datasets()

        # Interpret the data header and add required datasets.
        for dataset in datasets_definition:

            channel_name = dataset["name"]
            channel_type = dataset["type"]
            channel_shape = dataset["shape"]

            channel_group_name = '/data/' + channel_name + "/"

            _logger.debug("Creating datasets for channel_name '%s', type=%s, shape=%s.",
                          channel_name, channel_type, channel_shape)

            self.h5_writer.add_dataset(channel_group_name + 'pulse_id', dataset_group_name='pulse_id', dtype='i8')
            self.h5_writer.add_dataset(channel_group_name + 'is_data_present', dataset_group_name='is_data_present',
                                       dtype='u1')

            self._setup_channel_data_dataset(channel_group_name, channel_type, channel_shape)

    def write_data(self, json_data):

        datasets_to_create = [{"name": entry["channel"]["name"],
                               "type": entry["configs"][0]["type"],
                               "shape": entry["configs"][0]["shape"]} for entry in json_data["meta"]]

        self._create_datasets(datasets_to_create)

        for pulse_data in json_data["data"]:

            values = (x["value"] for x in pulse_data)
            self.h5_writer.write(values, dataset_group_name='data')

            pulse_ids = (x["pulseId"] for x in pulse_data)
            self.h5_writer.write(pulse_ids, dataset_group_name='pulse_id')

            is_data_valid = [1 if data_point is not None else 0 for data_point in pulse_ids]
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

    def _get_channel_data_dataset_definition(self, dtype, shape):

        dataset_shape = [1] + shape[::-1]
        dataset_max_shape = [None] + shape[::-1]
        dataset_type = channel_type_deserializer_mapping[dtype][0]

        if dtype == "string":
            dataset_shape = [1]
            dataset_max_shape = [None]
            dataset_type = h5py.special_dtype(vlen=str)

        return dataset_type, dataset_shape, dataset_max_shape

    def _setup_channel_data_dataset(self, channel_group_name, channel_type, channel_shape):

        dtype, dataset_shape, dataset_max_shape = self._get_channel_data_dataset_definition(channel_type, channel_shape)

        self.h5_writer.add_dataset(channel_group_name + 'data', dataset_group_name='data', shape=dataset_shape,
                                   maxshape=dataset_max_shape, dtype=dtype)

    def close(self):
        self.h5_writer.close_file()
