import argparse
import json
import datetime
import os
import h5py
import numpy as np

def run():

    parser = argparse.ArgumentParser(description='check consistency of produced files')

    parser.add_argument("-r", "--run_file", help="JSON file from the retrieve process", default=None)
    parser.add_argument("--frequency_reduction_factor", help="beam rate, default 1 means 100Hz (2: 50Hz, 4: 25Hz....) (overwrites one from json file)", default=0, type=int)

    args = parser.parse_args() 

    result = check_consistency(run_file=args.run_file, rate_multiplicator=args.frequency_reduction_factor)
      
    print("Result of consistency check (summary) : %s " % result["check"])
    if result["check"]:
        print("    OK : %s" % result["reason"])
    else:
        for reason in result["reason"]:
            print("    Reason : %s " % reason)

def check_consistency(run_file=None, rate_multiplicator=0):

    problems = []

    if run_file is None:
        problems.append("provide a json run file")
        return {"check" : False, "reason" : problems}

    if not os.path.exists(run_file):
        problems.append(f'{run_file} does not exist')
        return {"check" : False, "reason" : problems}

    try:
        with open(run_file) as json_file:
            parameters = json.load(json_file)
    except:
        problems.append("Can't read provided run file, may be not json?")
        return {"check" : False, "reason" : problems}
      
    start_pulse_id = parameters["start_pulseid"]
    stop_pulse_id  = parameters["stop_pulseid"]

    if rate_multiplicator == 0:
        if "rate_multiplicator" in parameters:
            rate_multiplicator = parameters["rate_multiplicator"]
        else:
            rate_multiplicator = 1

    pgroup = parameters["pgroup"]
    beamline = parameters["beamline"]
    run_number = parameters["run_number"]
    request_time = datetime.datetime.strptime(parameters["request_time"], '%Y-%m-%d %H:%M:%S.%f')

    full_directory = f'/sf/{beamline}/data/{pgroup}/raw/'
    if "directory_name" in parameters:
        full_directory = f'{full_directory}{parameters["directory_name"]}'
   
# todo make this check possible for different from 100Hz case (not straitforward - start_pulse_id can be not alligned properly with the rate)
# this is case for 100Hz:
    expected_pulse_id = []
    for p in range(start_pulse_id,stop_pulse_id+1):
        if p%rate_multiplicator == 0:
            expected_pulse_id.append(p)
    expected_number_measurements = len(expected_pulse_id)

    if "channels_list" in parameters:
        bsread_file = f'{full_directory}/run_{run_number:06}.BSREAD.h5'
        if not os.path.exists(bsread_file):
            problems.append(f'bsread file {bsread_file} does not exist')
        else:
            try:
                bsread_h5py = h5py.File(bsread_file,"r")
                inside_file = list(bsread_h5py.keys())
                if 'data' not in inside_file:
                    problems.append(f'BSREAD file {bsread_file} has bad content {inside_file}')
                else:
                    channels_inside_file = list(bsread_h5py['data'].keys())
                    for channel in parameters["channels_list"]:
                        if channel not in channels_inside_file:
                            problems.append(f'channel {channel} requested but not present in cameras file')
                        else:
                            pulse_id_raw     = bsread_h5py[f'/data/{channel}/pulse_id'][:]
                            is_data_present = bsread_h5py[f'/data/{channel}/is_data_present'][:]
                            # pulse_id = pulse_id_raw[is_data_present]
                            pulse_id = []
                            for n_p,p in enumerate(pulse_id_raw):
                                if p%rate_multiplicator == 0 and is_data_present[n_p]:
                                    pulse_id.append(p)
                            n_pulse_id = len(pulse_id)
                            if n_pulse_id != expected_number_measurements:
                                problems.append(f'{channel} number of pulse_id is different from expected : {n_pulse_id} vs {expected_number_measurements}')
                            else:
                                if pulse_id[0] != expected_pulse_id[0] or pulse_id[-1] != expected_pulse_id[-1]:
                                    problems.append(f'{channel} start/stop pulse_id are not the one which are requested (requested : {expected_pulse_id[0]},{expected_pulse_id[-1]}, got: {pulse_id[0]},{pulse_id[-1]}) ')
                                pulse_id_check = True # this is for 100Hz only, todo: to make for different rate
                                for i in range(n_pulse_id):
                                    if pulse_id[i] != expected_pulse_id[i]:
                                        pulse_id_check = False
                                        #print(channel, i, pulse_id[i], expected_pulse_id[i])
                                if not pulse_id_check:
                                    problems.append(f'{channel} pulse_id are not monotonic')
                bsread_h5py.close()
            except:
                problems.append(f'Can not read from BSREAD file {bsread_file} may be too early')


    if "camera_list" in parameters:
        cameras_file = f'{full_directory}/run_{run_number:06}.CAMERAS.h5'
        if not os.path.exists(cameras_file):
            problems.append(f'camera file {cameras_file} does not exist')
        else:
            try:
                cameras_h5py = h5py.File(cameras_file,"r")
                cameras_inside_file = list(cameras_h5py.keys())
                for camera in parameters["camera_list"]:
                    if camera not in cameras_inside_file:
                        problems.append(f'camera {camera} requested but not present in cameras file')
                    else:
                        pulse_id      = cameras_h5py[f'/{camera}/pulse_id'][:] 
                        n_pulse_id = len(pulse_id)
                        if n_pulse_id != expected_number_measurements:
                            problems.append(f'{camera} number of pulse_id is different from expected : {n_pulse_id} vs {expected_number_measurements}')
                        else:
                            if expected_pulse_id[0] != pulse_id[0] or expected_pulse_id[-1] != pulse_id[-1]:
                                problems.append(f'{camera} start/stop pulse_id are not the one which are requested')
                            pulse_id_check = True # this is for 100Hz only, todo: to make for different rate
                            for i in range(n_pulse_id):
                                if pulse_id[i] != expected_pulse_id[i]:
                                    pulse_id_check = False
                            if not pulse_id_check:
                                problems.append(f'{camera} pulse_id are not monotonic')
                        n_images_corrupted = 0
                        image_data = cameras_h5py[f'/{camera}/data']
                        for i_image in range(n_pulse_id):
                            try:
                                image_try = image_data[i_image]
                            except:
                                n_images_corrupted += 1
                        if n_images_corrupted != 0:
                            problems.append(f'{camera} {n_images_corrupted} images (from {n_pulse_id}) corrupted, can not read them')
                cameras_h5py.close()
            except:
                problems.append(f'Can not read from cameras file {cameras_file} may be too early')

    if "detectors" in parameters:
        for detector in parameters["detectors"]:

            detector_file = f'{full_directory}/run_{run_number:06}.{detector}.h5'
            if not os.path.exists(detector_file):
                problems.append(f'detector file {detector_file} does not exist') 
            else:
                try:
                    detector_h5py = h5py.File(detector_file,"r")
                    pulse_id      = detector_h5py[f'/data/{detector}/pulse_id'][:]
                    n_pulse_id = len(pulse_id)
# in case of converted data, frame_index, is_good_frame and daq_rec may be missing
                    if f'data/{detector}/frame_index' in detector_h5py.keys():
                        frame_index   = detector_h5py[f'data/{detector}/frame_index'][:]
                    else:
                        frame_index = [0] * n_pulse_id
                    if f'/data/{detector}/is_good_frame' in detector_h5py.keys():
                        is_good_frame = detector_h5py[f'/data/{detector}/is_good_frame'][:]
                    else:
                        is_good_frame = [1] * n_pulse_id
                    if f'/data/{detector}/daq_rec' in detector_h5py.keys():
                        daq_rec       = detector_h5py[f'/data/{detector}/daq_rec'][:]
                    else:
                        daq_rec = [0] * n_pulse_id

                    if len(frame_index) != n_pulse_id or len(is_good_frame) != n_pulse_id or len(daq_rec) != n_pulse_id:
                        problems.append(f'{detector} length of frame_index,is_good_frame,daq_rec is not consistent with pulse_id')
                    if n_pulse_id != expected_number_measurements:
                        problems.append(f'{detector} number of pulse_id is different from expected : {n_pulse_id} vs {expected_number_measurements}')
                    else:
                        if expected_pulse_id[0] != pulse_id[0] or expected_pulse_id[-1] != pulse_id[-1]:
                            problems.append(f'{detector} start/stop pulse_id are not the one which are requested')
                    # todo: check on nan's for pulse_id's
                        frame_index_check = True
                        n_frames_bad   = 0
                        pulse_id_check = True # this is for 100Hz only, todo: to make for different rate
                        for i in range(n_pulse_id):
                            if is_good_frame[i] != 1:
                                n_frames_bad += 1
                            else:
                                #if frame_index[i] != (frame_index[0]+i):
                                #    frame_index_check = False
                                if pulse_id[i] != expected_pulse_id[i]:
                                    pulse_id_check = False
                        if not frame_index_check:
                            problems.append(f'{detector} frame_index is not monotonic')
                        if n_frames_bad != 0:
                            problems.append(f'{detector} there are bad frames : {n_frames_bad} out of {n_pulse_id}') 
                        if not pulse_id_check:
                            problems.append(f'{detector} pulse_id are not monotonic')
                    detector_h5py.close()
                except:
                    problems.append(f'Can not read from detector file {detector_file} may be too early')
       
 
    if len(problems) > 0:
        return {"check" : False, "reason" : problems}
    else:
        return {"check" : True, "reason" : "all tests passed"} 

if __name__ == "__main__":
    run()

