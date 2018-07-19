from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import spotpy
import spotpy_setup_MONICA
import csv
from datetime import date
import numpy as np
from collections import defaultdict
import sys
import json

paths = {
    "cluster-path": "/archiv-daten/md/data/",
    "local-path": "z:/data/"
}

config = {
    "server": "localhost",
    "push-port": "6666",
    "pull-port": "7777",
    "runs-file": "unique_combinations_test.csv",
    "rep": 15,
    "cal-method": "MLE"
}
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        k,v = arg.split("=")
        if k in config:
            config[k] = v

basepath = os.path.dirname(os.path.abspath(__file__))

#read unique combinations
unique_combos = []
with open(basepath + "/" + config["runs-file"]) as _:
    reader = csv.reader(_)
    next(reader, None) #skip header
    for row in reader:
        soil_id = row[0]
        meteo_id = row[1].replace("(", "").replace(")", "").replace(", ", "_")
        rot_id = row[2]
        env_file = soil_id + "_" + meteo_id + "_" + rot_id + ".json"
        profiles = []
        for i in range(4):
            ref_index = i * 3 + 3
            if row[ref_index] == "":
                continue
            prof = {}
            prof["Corg"] = float(row[ref_index])
            prof["Nlay"] = int(row[ref_index +2])
            profiles.append(prof)
        unique_combos.append((env_file, profiles))

for env_file, profiles in unique_combos:
    #read proper env
    with open(basepath + "/envs/" + env_file) as _:
        env = json.load(_)
        #set correct path to climate
        path_0 = env["pathToClimateCSV"][0].split("data/")[1]
        path_1 = env["pathToClimateCSV"][0].split("data/")[1]
        if config["server"] == "localhost":
            env["pathToClimateCSV"][0] = paths["local-path"] + path_0
            env["pathToClimateCSV"][1] = paths["local-path"] + path_1
        else:
            env["pathToClimateCSV"][0] = paths["cluster-path"] + path_0
            env["pathToClimateCSV"][1] = paths["cluster-path"] + path_1
        #modify end date (no need to simulate to 2050)
        env["csvViaHeaderOptions"]["end-date"] = "2004-12-31"
        env["customId"] = env_file.replace(".json", "")

    #define params
    params=[]
    for i in range(len(profiles)):
        p={}
        p["name"] = "Corg_" + str(i)
        p["low"] = 0
        p["high"] = 2 * profiles[i]["Corg"]
        p["stepsize"] = 0.5
        p["optguess"] = profiles[i]["Corg"]
        p["minbound"] = 0
        p["maxbound"] = 2 * profiles[i]["Corg"]
        params.append(p)

    #create observation list
    obslist = []
    for i in range(len(profiles)):
        obslist.append(profiles[i]["Corg"]/100) #from % to kg kg-1

    #customize events section for output
    env["events"] = []
    time_aggr = {
            "from": "2000-01-01",
            "to": "2004-12-31"
        }
    last_lay = 1
    for i in range(len(profiles)):
        out_req = []
        var = "SOC"
        lay_aggr = [last_lay, 
                    min(last_lay + profiles[i]["Nlay"], 20),
                    "AVG"]
        out_req.append(var)
        out_req.append(lay_aggr)
        env["events"].append(time_aggr)
        env["events"].append([out_req])


spot_setup = spotpy_setup_MONICA.spot_setup(params, obslist, config, env)
results = []

if config["cal-method"] == "SCE-UA":
    sampler = spotpy.algorithms.sceua(spot_setup, dbname='SCEUA', dbformat='ram')
    sampler.sample(repetitions=config["rep"], ngs=len(params)+1, kstop=10, pcento=10, peps=10)
elif config["cal-method"] == "MLE":
    sampler = spotpy.algorithms.mle(spot_setup, dbname='MLE_CMF', dbformat='ram')
    sampler.sample(repetitions=config["rep"])

results.append(sampler.getdata())

best = sampler.status.params

'''
with open('optimizedparams.csv', 'wb') as outcsvfile:
    writer = csv.writer(outcsvfile)        
    for i in range(len(best)):
        outrow=[]
        arr_pos = ""
        if params[i]["array"].upper() != "FALSE":
            arr_pos = params[i]["array"]        
        outrow.append(params[i]["name"]+arr_pos)
        outrow.append(best[i])
        writer.writerow(outrow)
    if len(params) > len(best):
        reminder = []
        reminder.append("Don't forget to calculate and set derived params!")
        writer.writerow(reminder)
    text='optimized parameters saved!'
    print(text)
'''

print("finished!")



