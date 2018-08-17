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
import numpy as np

def set_param(params_list, p_name, p_value):
    #check nested structure
    add_index = False
    if isinstance(params_list[p_name], list) and isinstance(params_list[p_name][1], basestring):
        add_index = True
    #set val
    if not add_index:
        params_list[p_name] = p_value
    else:
        params_list[p_name][0] = p_value

paths = {
    "cluster-path": "/archiv-daten/md/data/",
    "local-path": "z:/data/",
    "env-path": "Z:/projects/sustag/spinup-version/"
}

config = {
    "server": "localhost",
    "push-port": "6666",
    "pull-port": "7777",
    "runs-file": "unique_combinations_OID.csv",
    "start-row": 4600,
    "end-row": 4600,
    "rep": 20,
    "cal-method": "MLE"
}
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        k,v = arg.split("=")
        if k in config:
            config[k] = v

basepath = os.path.dirname(os.path.abspath(__file__))
#envpath = os.path.dirname(basepath)
envpath = paths["env-path"]

outfile = basepath + "/optparams" + "_" + config["runs-file"]
with open(outfile, 'wb') as _:
    writer = csv.writer(_)
    header = ["params_id", "sim_id", "RMSE (% Corg)", "Corg_0", "Corg_1", "Corg_2", "Corg_3", "Corg_4"]
    writer.writerow(header)

#read unique combinations
unique_combos = []
with open(basepath + "/" + config["runs-file"]) as _:
    reader = csv.reader(_)
    next(reader, None) #skip header
    row_count = 0
    for row in reader:
        row_count +=1
        if row_count < config["start-row"]:
            continue
        if row_count > config["end-row"]:
            continue
        soil_id = row[0]
        meteo_id = row[1].replace("(", "").replace(")", "").replace(", ", "_")
        rot_id = row[2]
        orgNkreise = str(int(float(row[3])))
        env_file = soil_id + "_" + meteo_id + "_" + rot_id + "_" +  orgNkreise + ".json"
        profiles = []
        for i in range(4):
            ref_index = i * 3 + 4
            if ref_index > (len(row) - 1) or row[ref_index] == "":
                continue
            prof = {}
            prof["Corg"] = float(row[ref_index])
            prof["Nlay"] = int(row[ref_index +2])
            profiles.append(prof)
        unique_combos.append((env_file, profiles))

#read set of relevant and uncertain params
uncertain_params = np.genfromtxt(basepath + "/sample_15.csv", dtype=float, delimiter=',', names=True)

for env_file, profiles in unique_combos:
    print("calibrating " + str(env_file))
    #read proper env
    with open(envpath + "/dumped_envs/" + env_file) as _:
        env = json.load(_)
        #set correct path to climate
        path_0 = env["pathToClimateCSV"][0].split("data/")[1]
        path_1 = env["pathToClimateCSV"][1].split("data/")[1]
        if config["server"] == "localhost" or config["server"] == "10.10.26.34":
            env["pathToClimateCSV"][0] = paths["local-path"] + path_0
            env["pathToClimateCSV"][1] = paths["local-path"] + path_1
        else:
            env["pathToClimateCSV"][0] = paths["cluster-path"] + path_0
            env["pathToClimateCSV"][1] = paths["cluster-path"] + path_1
        #modify end date (no need to simulate to 2050)
        env["csvViaHeaderOptions"]["start-date"] = "1971-01-01"
        env["csvViaHeaderOptions"]["end-date"] = "2004-12-31"
        env["customId"] = env_file.replace(".json", "")
    
    #exclude layers below max mineralization depth
    active_layers = int(env["params"]["userSoilOrganicParameters"]["DEFAULT"]["MaxMineralisationDepth"] * 10)
    calib_layers = []
    n_lay = 0
    for prof in profiles:
        if n_lay < active_layers:
            calib_layers.append(True)
        else:
            calib_layers.append(False)
        n_lay += prof["Nlay"]

    #define params to be optimized
    params=[]
    for i in range(len(profiles)):
        if not calib_layers[i]:
            continue
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
        if not calib_layers[i]:
            continue
        obslist.append(profiles[i]["Corg"])

    #customize events section for output
    env["events"] = []
    time_aggr = {
            "from": "2000-01-01",
            "to": "2004-12-31"
        }
    upper_lay = 1
    for i in range(len(profiles)):
        if not calib_layers[i]:
            continue
        out_req = []
        var = "SOC"
        lower_lay = min(upper_lay + profiles[i]["Nlay"] - 1, 20)
        lay_aggr = [upper_lay,
                    lower_lay,
                    "AVG"]
        out_req.append(var)
        out_req.append(lay_aggr)
        env["events"].append(time_aggr)
        env["events"].append([out_req])
        upper_lay = lower_lay + 1
    
    #with open("dumped_events.json", "wb") as _:
    #    _.write(json.dumps(env["events"], indent=4))
    
    params_id = -1
    for p_set in uncertain_params:
        params_id += 1
        #debug
        #if params_id != 14:
        #    continue
        print("optimizing for parameter set: " + str(params_id))
        soil_params = [
            {
            "name": "PartSOM_Fast_to_SOM_Slow",
            "value": p_set[1]
            },
            {
            "name": "SOM_FastDecCoeffStandard",
            "value": p_set[2]
            },
            {
            "name": "PartSMB_Fast_to_SOM_Fast",
            "value": p_set[3]
            },
            {
            "name": "PartSMB_Slow_to_SOM_Fast",
            "value": p_set[4]
            },
            {
            "name": "SOM_SlowDecCoeffStandard",
            "value": p_set[6]
            },
            {
            "name": "AOM_SlowUtilizationEfficiency",
            "value": p_set[7]
            }
        ]
        fert_params = [
            {
            "name": "AOM_SlowDecCoeffStandard",
            "value": p_set[5]
            },
            #following two params were calibrated using exps 25 and 35 from V140 Muencheberg
            {
            "name": "PartAOM_to_AOM_Fast",
            "value": 0.81
            },
            {
            "name": "PartAOM_to_AOM_Slow",
            "value": 0.19
            }
        ]
        #customize environment:
        for soil_p in soil_params:
            set_param(env["params"]["userSoilOrganicParameters"]["DEFAULT"], soil_p["name"], soil_p["value"])
        for cm in env["cropRotations"][0]["cropRotation"]:
            for ws in cm["worksteps"]:
                if ws["type"] == "OrganicFertilization":
                    for fert_p in fert_params:
                        set_param(ws["parameters"], fert_p["name"], fert_p["value"])
        
        #with open("dumped_" + env_file, "wb") as _:
        #    _.write(json.dumps(env, indent=4))
        
        spot_setup = spotpy_setup_MONICA.spot_setup(params, obslist, config, env)
        results = []

        if config["cal-method"] == "SCE-UA":
            sampler = spotpy.algorithms.sceua(spot_setup, dbname='SCEUA', dbformat='ram')
            sampler.sample(repetitions=config["rep"], ngs=len(params)+1, kstop=10, pcento=10, peps=10)
        elif config["cal-method"] == "MLE":
            sampler = spotpy.algorithms.mle(spot_setup, dbname='MLE_CMF', dbformat='ram')
            sampler.sample(repetitions=config["rep"])

        spot_setup.monica_model.close_sockets()

        best_params = sampler.status.params
        RMSE = -sampler.status.objectivefunction

        with open(outfile, 'ab') as _:
            writer = csv.writer(_)
            row = []
            row.append(params_id)
            row.append(env_file.replace(".json", ""))
            row.append(round(RMSE, 4))
            for i in range(len(best_params)):
                row.append(round(best_params[i], 4))
            writer.writerow(row)

print("finished!")



