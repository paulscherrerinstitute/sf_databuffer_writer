import logging

import h5py
import numpy
from bsread.data.serialization import channel_type_deserializer_mapping
from bsread.writer import Writer
import data_api
import numpy as np

_logger = logging.getLogger(__name__)


DATA_DATASET_NAME = "data"


class DataBufferH5Writer(object):
    def __init__(self, output_file, parameters):
        self.output_file = output_file
        self.parameters = parameters

        self.h5_writer = Writer()
        self.h5_writer.open_file(self.output_file)

    def _build_pandas_data_frame(self, data, **kwargs):
        import pandas
        # for nicer printing
        pandas.set_option('display.float_format', lambda x: '%.3f' % x)

        index_field = kwargs['index_field']

        data_frame = None

        # Same as query["fields"] except "value"
        metadata_fields = ["pulseId", "globalDate"]

        for channel_data in data:
            if not channel_data['data']:  # data_entry['data'] is empty, i.e. []
                # No data returned
                _logger.warning("no data returned for channel %s" % channel_data['channel']['name'])
                # Create empty pandas data_frame
                tdf = pandas.DataFrame(columns=[index_field, channel_data['channel']['name']])
            else:
                if isinstance(channel_data['data'][0]['value'], dict):
                    # Server side aggregation
                    entry = []
                    keys = sorted(channel_data['data'][0]['value'])

                    for x in channel_data['data']:
                        entry.append([x[m] for m in metadata_fields] + [x['value'][k] for k in keys])
                    columns = metadata_fields + [channel_data['channel']['name'] + ":" + k for k in keys]

                else:
                    # No aggregation
                    entry = []
                    for data_entry in channel_data['data']:
                        entry.append([data_entry[m] for m in metadata_fields] + [data_entry['value']])
                    # entry = [[x[m] for m in metadata_fields] + [x['value'], ] for x in data_entry['data']]
                    columns = metadata_fields + [channel_data['channel']['name']]

                tdf = pandas.DataFrame(entry, columns=columns)
                tdf.drop_duplicates(index_field, inplace=True)

                # TODO check if necessary
                # because pd.to_numeric has not enough precision (only float 64, not enough for globalSeconds)
                # does 128 makes sense? do we need nanoseconds?
                conversions = {"pulseId": np.int64}
                for col in tdf.columns:
                    if col in conversions:
                        tdf[col] = tdf[col].apply(conversions[col])

            if data_frame is not None:
                data_frame = pandas.merge(data_frame, tdf, how="outer")  # Missing values will be filled with NaN
            else:
                data_frame = tdf

        if data_frame.shape[0] > 0:
            # dataframe is not empty

            # Apply milliseconds rounding
            # this is a string manipulation !
            if "globalSeconds" in data_frame.columns:
                data_frame["globalNanoseconds"] = data_frame.globalSeconds.map(lambda x: int(x.split('.')[1][3:]))
                data_frame["globalSeconds"] = data_frame.globalSeconds.map(lambda x: float(x.split('.')[0] + "." + x.split('.')[1][:3]))
            # Fix pulseid to int64 - not sure whether this really works
            # data_frame["pulseId"] = data_frame["pulseId"].astype(np.int64)

            data_frame.set_index(index_field, inplace=True)
            data_frame.sort_index(inplace=True)

        return data_frame


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

            #self.h5_writer.add_dataset(channel_group_name + 'pulse_id', dataset_group_name='pulse_id', dtype='i8')
            #self.h5_writer.add_dataset(channel_group_name + 'is_data_present', dataset_group_name='is_data_present',
            #                           dtype='u1')

            #self._setup_channel_data_dataset(channel_group_name, channel_type, channel_shape)

    def write_data(self, json_data):

        #datasets_to_create = [{"name": entry["channel"]["name"],
        #                       "type": entry["configs"][0]["type"],
        #                       "shape": entry["configs"][0]["shape"]} for entry in json_data["meta"]]
        datasets_to_create = [{"name": entry["channel"]["name"],
                               "type": entry["configs"][0]["type"],
                               "shape": entry["configs"][0]["shape"]} for entry in json_data]
        datasets_meta = {}
        for d in datasets_to_create:
            datasets_meta[d["name"]] = d

        self._create_datasets(datasets_to_create)

        _logger.info("Started writing data to disk.")

        df = self._build_pandas_data_frame(json_data, index_field="globalDate")
        for ch in df.columns:
            if ch in ["pulseId", 'globalSeconds', 'globalDate', 'eventCount', 'globalNanoseconds']:
                continue
            #_logger.info("/data/" + ch + "/pulse_id")
            self.h5_writer.file["/data/" + ch + "/pulse_id"] = df["pulseId"].values
            #_logger.info("/data/" + ch + "/data")
            
            #self.h5_writer.file["/data/" + ch + "/data"] = df[ch].values
            self.h5_writer.file["/data/" + ch].create_dataset("data", 
                                                              shape=[df["pulseId"].count(), ] + datasets_meta[ch]["shape"], 
                                                              dtype=datasets_meta[ch]["type"])
            for i,d in enumerate(df[ch]):
                if df[ch].notnull().values[i]:
                    self.h5_writer.file["/data/" + ch + "/data"][i] = df[ch].iloc[i]
            #_logger.info("/data/" + ch + "/is_data_present")
            self.h5_writer.file["/data/" + ch + "/is_data_present"] = df[ch].notnull().values
        #for pulse_data in json_data["data"]:
        #
        #    pulse_ids = [x["pulseId"] for x in pulse_data]
        #    self.h5_writer.write(pulse_ids, dataset_group_name='pulse_id')
        #
        #    is_data_valid = [1 if data_point is not None else 0 for data_point in pulse_ids]
        #    self.h5_writer.write(is_data_valid, dataset_group_name='is_data_present')
        #
        #    values = [x["value"] for x in pulse_data]
        #    self.h5_writer.write(values, dataset_group_name='data')
        
        

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
