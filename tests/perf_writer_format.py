import json

from line_profiler import LineProfiler

from sf_databuffer_writer.writer_format import DataBufferH5Writer

data_file = "/Users/babic_a/tmp/perf_data.json"
json_data = json.load(open(data_file))

output_file = "ignore_output_new.h5"

parameters = {"general/created": "test",
              "general/user": "tester",
              "general/process": "test_process",
              "general/instrument": "mac",
              "output_file": "test.h5"}

writer = DataBufferH5Writer(output_file, parameters)
profiler = LineProfiler()
profiled_write = profiler(writer.write_data)

profiled_write(json_data)

writer.close()
profiler.print_stats()
