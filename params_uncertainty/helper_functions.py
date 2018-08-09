import os
import csv
from datetime import date
from collections import defaultdict
import numpy as np
import spotpy
import matplotlib.pyplot as plt

def common_datastructure():
    'returns data structure for observations and simulations. keys: treatment, year, parcels/data'
    return defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

def read_exp_map(filename, exps=None):
    'read experiments features'
    exp_maps = []
    basepath = os.path.dirname(os.path.abspath(__file__))
    with open(filename) as exp_mapfile:
        dialect = csv.Sniffer().sniff(exp_mapfile.read(), delimiters=';,\t')
        exp_mapfile.seek(0)
        reader = csv.reader(exp_mapfile, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            exp_map = {}
            exp_map["parcel"] = int(row[0])
            exp_map["treatment"] = int(row[5])
            exp_map["sim_file"] = basepath+"\\sim_files\\"+row[1]
            exp_map["crop_file"] = basepath+"\\crop_files\\"+row[2]
            exp_map["site_file"] = basepath+"\\site_files\\"+row[3]
            exp_map["climate_file"] = basepath+"\\climate_files\\"+row[4]
            if exps == None:
                exp_maps.append(exp_map)
            elif int(exp_map["treatment"]) in exps:
                exp_maps.append(exp_map)
    return exp_maps

def read_obs(filename, exps=None):
    'read observations'
    observations = common_datastructure()
    with open(filename) as obsfile:
        dialect = csv.Sniffer().sniff(obsfile.read(), delimiters=';,\t')
        obsfile.seek(0)
        reader = csv.reader(obsfile, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            if row[6].upper() == "Y":
                parcel = int(row[0])
                treatment = int(row[11])
                year = int(row[1])
                value = float(row[5])
                if exps == None:
                    observations[treatment][year]["parcels"].append(parcel)
                    observations[treatment][year]["data"].append(value)
                elif treatment in exps:
                    observations[treatment][year]["parcels"].append(parcel)
                    observations[treatment][year]["data"].append(value)
    return observations

def create_spotpylist(observations, simulations=None, treatment=None):
    'converts observations or simulations in the spotpy format'
    spotpylist = []
    for tr in sorted(observations.keys()):
        if treatment != None and tr != treatment:
            #used to filter for specific treatment
            continue
        for year in sorted(observations[tr].keys()):
            #print((tr, year))
            if simulations == None:
                #create obslist
                data = np.array(observations[tr][year]["data"])
            else:
                #create simlist
                data = np.array(simulations[tr][year]["data"])
            spotpylist.append(float(np.mean(data)))
    return spotpylist

def read_params(filename):
    'read params to be calibrated'
    params = []
    with open(filename) as paramscsv:
        dialect = csv.Sniffer().sniff(paramscsv.read(), delimiters=';,\t')
        paramscsv.seek(0)
        reader = csv.reader(paramscsv, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            p={}
            p["name"] = row[0]
            p["low"] = float(row[1])
            p["high"] = float(row[2])
            p["optguess"] = float(row[3])
            p["avg"] = float(row[4])
            p["st_dev"] = float(row[5])
            p["type"] = row[6]
            params.append(p)
    return params

def SOC_plot(observations, simulations, filename):    
    font = {
        'family' : 'calibri',
        'weight' : 'normal',
        'size'   : 18
    }
    
    plt.rc('font', **font)    
    n_subplots = max(2, len(observations.keys()))
    # N subplots, sharing x axis
    width = 20
    height = n_subplots * 10
    f, axarr = plt.subplots(n_subplots, sharex=False, figsize=(width, height))
    i=0
    for treatment in sorted(observations.keys()):
        #calculate indices
        obs_shortlist = create_spotpylist(observations, treatment=treatment)
        sim_shortlist = create_spotpylist(observations, simulations, treatment=treatment)
        RMSE = spotpy.objectivefunctions.rmse(obs_shortlist, sim_shortlist)
        EF = spotpy.objectivefunctions.nashsutcliffe(obs_shortlist, sim_shortlist)

        #boxplot observations
        years = sorted(observations[treatment].keys())
        box_data = []
        for yr in years:
            yr_data = observations[treatment][yr]["data"]
            box_data.append(yr_data)
        axarr[i].boxplot(box_data, positions=years)
        
        #line simulations
        years = sorted(simulations[treatment].keys())
        avgs = []
        mins = []
        maxs = []
        for yr in years:
            yr_data = simulations[treatment][yr]["data"]
            avgs.append(float(np.mean(yr_data)))
            mins.append(min(yr_data))
            maxs.append(max(yr_data))
        my_label = 'T ' + str(treatment) + ': RMSE=' + str(round(RMSE, 3)) + ' EF=' + str(round(EF, 3))
        axarr[i].plot(years, avgs, "-", label=my_label)
        axarr[i].fill_between(years, mins, maxs, alpha=0.2)
        axarr[i].legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
        
        i +=1

    f.savefig(filename)
    print('A figure has been saved as ' + filename)