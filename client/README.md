
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
- "detectors" : python dictionary, currently containing name of jungfrau detector (e.g. JF01T03V01) as key and empty dictionary as a value (to allow to send to retrieve additional parameters, like ROI, compression etc)
- "scan_info" : python dictionary to specify that this request belongs to a particular scan (if proper information is provided (for example see scan_step.json in this directory), the appropriate scan_info json file will be created inside raw/../scan_info/ directory (similar to what eco and run_control did in res/ directory))

 Successful request needs to have at least one list non-empty in request (otherwise there is nothing to ask to retrieve)

<a id="bookkeeping"></a>
### Bookkeeping

 In case of successful (accepted by broker) request, complete parameters used for it will be saved in a special directory on raw/.daq/ with name run_RUN_NUMBER.json:
```bash
root@sf-daq-1 .daq]# pwd
/sf/maloja/data/p18493/raw/.daq
[root@sf-daq-1 .daq]# cat run_000001.json 
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
 In addition we log in this .daq/ directory the output of the retrieve procedures (currently only for Jungfrau detectors, but plan is to do the same
for data, image buffer retrieval and cadump)

<a id="Example1"></a>
## Example1

<a id="Example2"></a>
## Example2

<a id="Check"></a>
## Check


