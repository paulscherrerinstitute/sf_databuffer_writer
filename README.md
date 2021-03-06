[![Build Status](https://travis-ci.org/paulscherrerinstitute/sf_databuffer_writer.svg?branch=master)](https://travis-ci.org/paulscherrerinstitute/sf_databuffer_writer/)

# SwissFEL DataBuffer Writer
Temporary writing solution for bsread data in SwissFEL DAQ system.

**NOTE**: This writer should not be used if sf_bsread_writer is available.

# Table of content
1. [Quick start](#quick_start)
2. [Build](#build)
    1. [Conda setup](#conda_setup)
    2. [Local build](#local_build)
3. [Running the servers](#running_the_servers)
4. [Web interface](#web_interface)
    1. [REST API](#rest_api)
5. [Audit Trail](#audit_trail)

<a id="quick_start"></a>
## Quick start

The writing solution is composed by 2 parts:

- Broker (service that waits for DIA to set acquisition parameters, and waits for the H5 writer to send the first 
and last pulse id.)
- Writer (receives the writing request from the broker over ZMQ and downloads + writes the data)

### Broker
**Entry point**: sf_databuffer_writer/broker.py

Broker runs as a systemd service **/etc/systemd/system/broker.service**. It writes the audit log of all the sent requests in 
**/var/log/sf\_databuffer\_audit.log** by default. This can be changed with the config.DEFAULT_AUDIT_FILENAME 
parameter.

The broker is supposed to run all the time. Even if writing from the data_api does not work, it is still useful 
to have an audit trail of requests you can repeat.

In case of problems with the communication between the broker and the writer, you can run the broker with the 
**--audit\_trail\_only** flag, which will prevent the sending out of requests over ZMQ (the requests will only be written 
in the config.DEFAULT_AUDIT_FILENAME file).

For more information on how to parse and re-acquire data from audit trail please check the 
[Audit Trail](#audit_trail) chapter.

### Writer
**Entry point**: sf_databuffer_writer/writer.py

Writer runs as a systemd service **/etc/systemd/system/broker\_writer1.service**. If for some reason it cannot 
write the requested data, it creates a request file called **output_file**.err. This is similar to the audit trail 
of the broker (it has the same format) and signals that something went wrong and data needs to be downloaded again.

Example:

- output_file = /sf/alvra/data/data1.h5
- request_file = /sf/alvra/data/data1.h5.err

The .err file will be created only in case of an error. For more information on how to parse and re-acquire data 
from .err files please check the [Audit Trail](#audit_trail) chapter.

The writer is supposed to run all the time - you can also have more than 1 writer - you need more systemd services.
Just duplicate the /etc/systemd/system/broker_writer1.service multiple times. The communication between broker 
and writer is push-pull (round robin), so multiple writers can be used for load balancing.

**NOTE**: You set the user_id under which the writer is running in the /home/writer/start_broker_writer.sh (you need 
to restart the service for the changes to take effect). The user_id is the second parameter (-1 by default). Please
note that this is really the user_id and not the username.

<a id="build"></a>
## Build

<a id="conda_setup"></a>
### Conda setup
If you use conda, you can create an environment with the sf_databuffer_writer library by running:

```bash
conda create -c paulscherrerinstitute --name <env_name> sf_databuffer_writer
```

After that you can just source you newly created environment and start using the server.

<a id="local_build"></a>
### Local build
You can build the library by running the setup script in the root folder of the project:

```bash
python setup.py install
```

or by using the conda also from the root folder of the project:

```bash
conda build conda-recipe
conda install --use-local sf_databuffer_writer
```

#### Requirements
The library relies on the following packages:

- bsread >=1.1.0
- bottle
- requests
- data_api

In case you are using conda to install the packages, you might need to add the **paulscherrerinstitute** channel to
your conda config:

```
conda config --add channels paulscherrerinstitute
```

<a id="running_the_servers"></a>
## Running the servers


<a id="web_interface"></a>
## Web interface

All request (with the exception of **start\_pulse\_id**, **stop\_pulse\_id**, and **kill**) return a JSON 
with the following fields:
- **state** - \["ok", "error"\]
- **status** - What happened on the server or error message, depending on the state.
- Optional request specific field - \["statistics", "parameters"]

<a id="rest_api"></a>
### REST API
In the API description, localhost and port 8888 are assumed. Please change this for your specific case.

* `GET localhost:8888/status` - get the status of the broker.

* `POST localhost:8888/parameters` - set parameters of the broker.
    - Response specific field: "parameters" - Parameters you just set.  

* `GET localhost:8888/stop` - stop the broker.

* `GET localhost:8888/kill` - kill the broker process.
    - Empty response.

* `GET localhost:8888/statistics` - get broker process statistics.
    - Response specific field: "statistics" - Data about the writer.

* `PUT localhost:8888/start_pulse_id/<pulse_id>` - set first pulse_id to write to the output file.
    - Empty response.

* `PUT localhost:8888/stop_pulse_id/<pulse_id>` - set last pulse_id to write to the output file.
    - Empty response.

<a id="audit_trail"></a>  
## Audit Trail

The audit trail (for the broker) and the .err file (for the writer) have the same format. Each line in the 
audit trail of the broker (the .err file has always only 1 line) is:
```
# Timestamp         Writing request JSON String
[%Y%m%d-%H%M%S]     {"parameters": "{\"general/instrument\": \"Alvra, JF 4.5M\", \"general/created\":....
```

To read and interpret this line with Python:
```python
import json

# The filename can be replaced with an .err file.
with open("test_bsread.h5.err") as audit_trail:
    lines = audit_trail.readlines()
    
for line in lines:
    timestamp = line[1:16]
    json_string = line[18:]
    
    # This writing request can be manually submitted to the writer, to try writing the file manually.
    writing_request = json.loads(json_string)
    
    data_api_request = json.loads(writing_request["data_api_request"])
    parameters = json.loads(writing_request["parameters"])
    timestamp = writing_request["timestamp"]
    
    print(data_api_request)
    print(parameters)
    print(timestamp)
```
### Writing request
Writing request is a dictionary with 2 values:

- parameters (parameters for the H5 writer)
- data_api_request (request sent to the data api to get the data back)

This 2 values define all the properties needed to download the data at any time.

### Writing files from audit trail manually
Files can be written using the **sf_databuffer_writer.writer.get_data_from_buffer** and 
**sf_databuffer_writer.writer.write_data_to_file** method.

```python
from sf_databuffer_writer.writer import get_data_from_buffer, write_data_to_file

# You should load this from the audit trail or from the .err file.
parameters = {}
data_api_request = {}

data = get_data_from_buffer(data_api_request)
write_data_to_file(parameters)
```