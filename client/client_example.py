import time
import sys
import signal
import datetime
import os

import daq_client 

pulseid = {
  "alvra"   : "SLAAR11-LTIM01-EVR0:RX-PULSEID",
  "bernina" : "SLAAR21-LTIM01-EVR0:RX-PULSEID",
  "maloja"  : "SLAAR11-LTIM01-EVR0:RX-PULSEID"
}

def get_beamline():
    import socket
    ip2beamlines = {"129.129.242": "alvra", "129.129.243": "bernina", "129.129.246": "maloja"}
    ip=socket.gethostbyname(socket.gethostname())
    beamline = None
    if ip[:11] in ip2beamlines:
        return ip2beamlines[ip[:11]]

try:
    import epics
    epics_available=True
    PV_pulseid=epics.PV(pulseid[get_beamline()])
except:
    epics_available=False

def get_current_pulseid():
    if not epics_available:
        return get_fake_pulseid()
    else:
        try:
            p = int(PV_pulseid.get())
        except:
            p = get_fake_pulseid()
        return p

def get_fake_pulseid():
    #2020-05-08 08:29:52.742737 : 11718049010
    # 28.05.2020 - checked compared to "real" pulse-id:  2380 pulse's difference
    reference_date = datetime.datetime(2020, 5, 8, 8, 29, 52)
    now = datetime.datetime.utcnow()
    delta = (datetime.datetime.utcnow()-reference_date).total_seconds()*1000
    return int(delta/10)+11718049010 + 2361 

class BrokerClient:

    def __init__(self, pgroup):
        self.last_run = 0
        self.pgroup = pgroup

        self.rate_multiplicator = 1

        self.output_directory = None
        self.channels_file = None
        self.epics_file = None
        self.detectors_file = None
        self.scan_step_info_file = None

        self.start_pulseid = None

        beamline=get_beamline()
        raw_directory = f'/sf/{beamline}/data/{self.pgroup}/raw/'
        if not os.path.isdir(raw_directory):
            raise NameError(f'{raw_directory} doesnt exist or accessible')

    def configure(self, output_directory=None, 
                  channels_file=None, epics_file=None, 
                  detectors_file=None, scan_step_info_file=None,
                  rate_multiplicator=1):

        self.output_directory = output_directory
        self.channels_file    = channels_file
        self.epics_file       = epics_file
        self.detectors_file   = detectors_file
        self.scan_step_info_file = scan_step_info_file
        self.rate_multiplicator = rate_multiplicator

        try:
            beamline=get_beamline()
            last_run_file = f'/sf/{beamline}/data/{self.pgroup}/raw/run_info/LAST_RUN'
            if os.path.exists(last_run_file):
                run_file = open(last_run_file, "r")
                self.last_run = int(run_file.read())
                run_file.close()
        except:
            pass

    def start(self):
        self.start_pulseid = get_current_pulseid()

    def stop(self, stop_pulseid=None):
        if self.start_pulseid is not None:
            if stop_pulseid is None:
                stop_pulseid = get_current_pulseid()
            last_run_previous = self.last_run
            self.last_run = daq_client.retrieve_data_from_buffer_files(pgroup=self.pgroup, output_directory=self.output_directory,
                        channels_file=self.channels_file, epics_file=self.epics_file,
                        detectors_file=self.detectors_file,
                        start_pulseid=self.start_pulseid, stop_pulseid=stop_pulseid,
                        rate_multiplicator=self.rate_multiplicator,
                        scan_step_info_file=self.scan_step_info_file)
            if self.last_run is None:
                self.last_run = last_run_previous
            self.start_pulseid = None
        else:
            print("Run was not started to stop it")

    def status(self):
        if self.start_pulseid is not None:
            return "running"
        return None

    def run(self, number_frames=1000):

        self.start()

        stop_pulseid = int(self.start_pulseid + number_frames*self.rate_multiplicator-1)

        last_known_run = int(self.last_run) if self.last_run is not None else -1
        def signal_handler(sig, frame):
            current_pulseid = get_current_pulseid()
            print('\nYou pressed Ctrl+C!')
            print(f'what do you want me to do with already collected up to now frames (pulseids: {self.start_pulseid}-{current_pulseid})')
            answer=input('[s]-save them into; any other key - discard : ')
            if answer == 's':
               self.stop(stop_pulseid=current_pulseid)  
            raise NameError("Ctrl-c is called")
        signal.signal(signal.SIGINT, signal_handler)
        try:
            while True:
                current_pulseid = get_current_pulseid() 
                if current_pulseid >= stop_pulseid:
                    break
                time.sleep(0.1)
                progress = ((current_pulseid-self.start_pulseid)/(stop_pulseid-self.start_pulseid))
                block = int(round(20*progress))
                text = "\r[{0}] {1}% Run: {2}".format( "#"*block + "-"*(20-block), int(progress*100), last_known_run+1)
                sys.stdout.write(text)
                sys.stdout.flush()
            print()

            self.stop(stop_pulseid=stop_pulseid)
        except:
            raise
            
