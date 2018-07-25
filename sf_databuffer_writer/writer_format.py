import logging

import h5py
import numpy
from bsread.data.serialization import channel_type_deserializer_mapping

_logger = logging.getLogger(__name__)


class DataBufferH5Writer(object):
    def __init__(self, output_file, parameters):
        self.output_file = output_file
        self.parameters = parameters

        self.file = h5py.File(self.output_file, "w")

    def _prepare_format_datasets(self):

        _logger.info("Initializing format datasets.")

        self.file.create_dataset("/general/created",
                                 data=numpy.string_(self.parameters["general/created"]))

        self.file.create_dataset("/general/instrument",
                                 data=numpy.string_(self.parameters["general/instrument"]))

        self.file.create_dataset("/general/process",
                                 data=numpy.string_(self.parameters["general/process"]))

        self.file.create_dataset("/general/user",
                                 data=numpy.string_(self.parameters["general/user"]))

    def _build_datasets_data(self, json_data):

        pulse_ids = set()

        for channel_data in json_data:
            data = channel_data["data"]

            if not data:
                continue

            for data_point in data:
                pulse_ids.add(data_point["pulseId"])

        pulse_ids = sorted(pulse_ids)
        pulse_id_to_data_index = {data: index for index, data in enumerate(pulse_ids)}
        n_data_points = len(pulse_id_to_data_index)

        _logger.info("Built array of pulse_ids. n_data_points=%d" % n_data_points)

        datasets_data = {}

        for channel_data in json_data:
            name = channel_data["channel"]["name"]
            data = channel_data["data"]

            _logger.debug("Formatting data for channel %s." % name)

            try:

                channel_type = channel_data["configs"][0]["type"]
                channel_shape = channel_data["configs"][0]["shape"]

                dataset_type, dataset_shape = self._get_dataset_definition(channel_type, channel_shape, n_data_points)

                dataset_values = numpy.zeros(dtype=dataset_type, shape=dataset_shape)
                dataset_value_present = numpy.zeros(shape=(n_data_points,), dtype="bool")

                if data:
                    for data_point in data:
                        data_index = pulse_id_to_data_index[data_point["pulseId"]]

                        if len(channel_shape) > 1:
                            # Bsread is [X, Y] but numpy is [Y, X].
                            data_point["value"] = numpy.array(data_point["value"], dtype=dataset_type).\
                                reshape(channel_shape[::-1])

                        dataset_values[data_index] = data_point["value"]
                        dataset_value_present[data_index] = 1

                datasets_data[name] = {
                    "data": dataset_values,
                    "is_data_present": dataset_value_present
                }

            except:
                _logger.warning("Cannot convert channel_name %s. Is the channel is the data buffer?" % name)

        return pulse_ids, datasets_data

    def _get_dataset_definition(self, channel_dtype, channel_shape, n_data_points):

        dataset_type = channel_type_deserializer_mapping[channel_dtype][0]
        dataset_shape = [n_data_points] + channel_shape[::-1]

        if channel_dtype == "string":
            dataset_type = h5py.special_dtype(vlen=str)
            dataset_shape = [n_data_points] + channel_shape[::-1]

        return dataset_type, dataset_shape

    def write_data(self, json_data):

        self._prepare_format_datasets()

        _logger.info("Building numpy arrays with received data.")

        pulse_ids, datasets_data = self._build_datasets_data(json_data)

        _logger.info("Writing data to disk.")

        for name, data in datasets_data.items():
            self.file["/data/" + name + "/pulse_id"] = pulse_ids
            self.file["/data/" + name + "/data"] = data["data"]
            self.file["/data/" + name + "/is_data_present"] = data["is_data_present"]

    def close(self):
        self.file.close()
