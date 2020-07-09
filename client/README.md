
# Retrieve of data from the buffers

With move of detector to a buffer solution, the whole concept of data acquisition 
is reduced to reitrieve request from the buffers (data buffer for BS, image buffer for
the camera images, detector buffer for Jungfrau and archiver for cadump)

# Table of content
1. [Call to broker](#call_broker)
    1. [Call](#call)
    2. [Parameters](#parameters)
    3. [Bookkeeping](#bookkeeping)
2. [Example1](#example1)
3. [Example2](#example2)
4. [Check](#check)

<a id="call_broker"></a>
## Call to broker

Current broker is running on sf-daq-1 and serves for the whole Swissfel (Alva, Bernina, Maloja)

<a id="call"></a>
### Call

The following code is enough to make a call to current broker:
```python
import requests
broker_address = "http://sf-daq-1:10002"
TIMEOUT_DAQ = 10

r = requests.post(f'{broker_address}/retrieve_from_buffers',json=parameters, timeout=TIMEOUT_DAQ)
```

return object r is a dictionary with two keys: 'status' and 'message'. In case of no problem with the request to retrieve data (so request is accepted to be processed
by broker), 'status' is 'ok' and 'message' contains a RUN_NUMBER (incrementing number, individual for each pgroup). In case of problems, request is not accepted and 'status' is 'failed', 'message' contains a string with the description of the problem. (as an example of problems - wrong pgroup is specified, corresponding pgroup is closed for writing etc.) 

<a id="parameters"></a>
### Parameters
'parameters' passed in request is a dictionary.
There are very little numbers of mandatory key/values which needs to be present in the 'parameters' request, namely:
```
parameters["pgroup"] = "p12345"
parameters["start_pulseid"] = 1000 # be reasonable and change it to a proper one
parameters["stop_pulseid"]  = 2000 # corresponding to a time of test/use. This is a request to retrieve data from buffers, which have limited lifetime
```
 Failure to not provide one of these parameters will result in decline of the broker to retrieve data

 And number of optional parameters:
- "rate_multiplicator" : integer number, indicating what is the beam rate (or expectation for the source rate delivery), default is 1(means 100Hz), (2 - 50Hz, 4 - 25Hz; 10 - 10Hz, 20 - 5Hz, 100 - 1Hz). Currently setting or not this variable doesn't change anything in retrieve, but helps with the checks of the retrieve, see below
- "directory_name" : output directory where data will be written (relative to the raw directory of pgroup, so "dir/sample/test" would correspond to request to write to /sf/<beamline>/data/p12345/raw/dir/sample/test/)
- "channels_list" : python list with the source name from data buffer (don't put CAMERA's images here, but CAMERA processed parameters)
- "camera_list" : python list with name of CAMERA's (complete name, with :FPICTURE at the end)
- "pv_list" : python list with name of epics PV to retrieve from archiver by cadump
- "detectors" : python dictionary, containing name of jungfrau detector (e.g. JF01T03V01) as key and a dictionary with parameters as a value, see [Detector parameters](#detector_parameters) for available options
- "scan_info" : python dictionary to specify that this request belongs to a particular scan (if proper information is provided (for example see scan_step.json in this directory), the appropriate scan_info json file will be created inside raw/../scan_info/ directory (similar to what eco and run_control did in res/ directory))

 Successful request needs to have at least one list non-empty in request (otherwise there is nothing to ask to retrieve)

<a id="detector_parameters"></a>
#### Detector parameters
- `compression (bool)`: apply bitshuffle+lz4 compression, defaults to True
- `adc_to_energy (bool)`: apply gain and pedestal corrections, converting raw detector values into energy, defaults to True

The following parameters apply only when `conversion = True`, otherwise they are ignored:
- `mask (bool)`: perform masking of bad pixels (assign them to 0), defaults to True
- `mask_double_pixels (bool)`: also perform masking of double pixels (only applies if `mask = True`), defaults to True
- `geometry (bool)`: apply geometry correction, defaults to False
- `gap_pixels (bool)`: add gap pixels between detector chips, defaults to True
- `factor (float, None)`: divide all pixel values by a factor and round the result, saving them as int32, keep the original values and type if None, defaults to None

<a id="bookkeeping"></a>
### Bookkeeping

 In case of successful (accepted by broker) request, complete parameters used for it will be saved in a special directory on raw/run_info/ with name run_RUN_NUMBER.json (to not have too many files in one directory runs are splitted by 1000, so directory 003000/ contains information about runs with numbers 3000-3999):
```bash
# pwd
/sf/maloja/data/p18493/raw/run_info
# ls
000000  LAST_RUN
# cat 000000/run_000001.json 
{
  "pgroup": "p18493",
  "directory_name": "covid/detector_test2",
  "start_pulseid": 11884948775,
  "stop_pulseid": 11884949774,
  "channels_list": [
    "SAR-CVME-TIFALL5:EvtSet",
    "SAR-CVME-TIFALL4:EvtSet"
  ],
  "detectors": {
    "JF07T32V01": {}
  },
  "beamline": "maloja",
  "run_number": 1,
  "request_time": "2020-05-27 18:06:39.772622"
}
``` 
 In addition we log in this run_info/ directory the output of the retrieve procedures (currently only for Jungfrau detectors, but plan is to do the same
for data, image buffer retrieval and cadump)

<a id="Example1"></a>
## Example1

 Command line example how to use broker to request a retireve of data is daq_client.py. To run is enough to have python > 3.6 and standard packages (requests, os, json)
(so standard PSI python environment is good for this purpose):
```bash
$ module load psi-python36/4.4.0 
$ python daq_client.py -h
usage: daq_client.py [-h] [-p PGROUP] [-d OUTPUT_DIRECTORY] [-c CHANNELS_FILE]
                     [-e EPICS_FILE] [-f FILE_DETECTORS]
                     [-r RATE_MULTIPLICATOR] [-s SCAN_STEP_FILE]
                     [--start_pulseid START_PULSEID]
                     [--stop_pulseid STOP_PULSEID]

test broker

optional arguments:
  -h, --help            show this help message and exit
  -p PGROUP, --pgroup PGROUP
                        pgroup, example p12345
  -d OUTPUT_DIRECTORY, --output_directory OUTPUT_DIRECTORY
                        output directory for the data, relative path to the
                        raw directory in the pgroup
  -c CHANNELS_FILE, --channels_file CHANNELS_FILE
                        TXT file with list channels
  -e EPICS_FILE, --epics_file EPICS_FILE
                        TXT file with list of epics channels to save
  -f FILE_DETECTORS, --file_detectors FILE_DETECTORS
                        JSON file with the detector list
  -r RATE_MULTIPLICATOR, --rate_multiplicator RATE_MULTIPLICATOR
                        rate multiplicator (1(default): 100Hz, 2: 50Hz,)
  -s SCAN_STEP_FILE, --scan_step_file SCAN_STEP_FILE
                        JSON file with the scan step information
  --start_pulseid START_PULSEID
                        start pulseid
  --stop_pulseid STOP_PULSEID
                        stop pulseid

``` 

<a id="Example2"></a>
## Example2

 Another example is more "start/stop" oriented way of doing data acquistion. To run this example one needs, in addition to daq_config.py, script client_example.py.
It can also run in a standard PSI environment, but the pulse_id's would be wrong (the proper way to get a pulse_id is to use one of the channel which provide them
effectively, see client_example.py). So in case one run this example in environment without pyepics, the guessed, fake pulseid would be approximately ok (due to the lock to the 50Hz electricity frequency for accelerator, our 100Hz is not an ideal 100Hz, so it's impossible to make a 100% accurate prediction from time to pulse_id)
```bash
. /opt/gfa/python 3.7 # this loads proper environment with pyepics in it
$ ipython
Python 3.7.5 (default, Oct 25 2019, 15:51:11)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.2.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import client_example as client                                                                                                                                   

In [2]: daq_client = client.BrokerClient(pgroup="p12345")                                                                                                                 

In [3]: daq_client.configure(output_directory="test/daq", channels_file="channel_list", rate_multiplicator=2, detectors_file="jf_jf01.json")                              

In [4]: daq_client.run(1000)                                                                                                                                              
[####################] 99% Run: 2
success: run number(request_id) is 2
```

 Note that you can "Ctrl-C" during "run" execution, with it you'll be asked do you want to "record" data which you took from start till pressing "Ctrl-C"
which is an illustration of the principle of the retrieve-based daq strategy - run(with RUN_NUMBER) will exist only when request to retrieve data is made.
Data are already recorded and present in buffers.

<a id="Check"></a>
## Check

Since we record the request, which channel, detectors etc are asked to be retrieve, we provide also a check scipt, check.py. With it one can check if the result
of the retrieve is acceptable or some problems exists. Since different sources may run at a frequency different from beam or from each other, it may be 
normal to get value for each pulse_id from source running at 100Hz, though machine and daq acquisition is running at lower frequency. One can check result 
of retrieve against different then the machine frequency.
```
module load psi-python36/4.4.0

$ python check.py --help
usage: check.py [-h] [-r RUN_FILE]
                [--frequency_reduction_factor FREQUENCY_REDUCTION_FACTOR]

check consistency of produced files

optional arguments:
  -h, --help            show this help message and exit
  -r RUN_FILE, --run_file RUN_FILE
                        JSON file from the retrieve process
  --frequency_reduction_factor FREQUENCY_REDUCTION_FACTOR
                        beam rate, default 1 means 100Hz (2: 50Hz, 4:
                        25Hz....) (overwrites one from json file)

$ python check.py -r /sf/alvra/data/p18390/raw/run_info/000000/run_000151.json 
Result of consistency check (summary) : False 
    Reason : SARES11-SPEC125-M1.processing_parameters number of pulse_id is different from expected : 998 vs 1000 
    Reason : SARES11-SPEC125-M1.roi_background_x_profile number of pulse_id is different from expected : 998 vs 1000 
    Reason : SARES11-SPEC125-M1.roi_signal_x_profile number of pulse_id is different from expected : 998 vs 1000 
    Reason : SARES11-SPEC125-M1:FPICTURE number of pulse_id is different from expected : 998 vs 1000 


```

