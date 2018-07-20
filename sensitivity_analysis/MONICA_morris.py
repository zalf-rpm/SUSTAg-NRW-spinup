import json
import sys
import monica_io
import zmq
from SALib.sample import morris as morris_pler
from SALib.analyze import morris as morris_lyze
import numpy as np
import threading
from threading import Thread
import csv
import os
import pandas

basepath = os.path.dirname(os.path.abspath(__file__))
envpath = os.path.dirname(basepath)

paths = {
    "cluster-path": "/archiv-daten/md/data/",
    "local-path": "z:/data/"
}

config = {
    "server": "cluster3", #"localhost",
    "push-port": "6666",
    "pull-port": "7777",
    "runs-file": "unique_combinations_test.csv",
    "write-all-out": True
}

if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        k,v = arg.split("=")
        if k in config:
            config[k] = v

class FuncThread(threading.Thread):
    def __init__(self, target, *args):
        self._target = target
        self._args = args
        threading.Thread.__init__(self)
 
    def run(self):
        self._target(*self._args)

def collect_results(num_of_sims):
    received_results = 0
    leave = False
    while not leave:
        try:
            out = pull_socket.recv_json()
        except:
            continue

        SOC_ini = out["data"][0]["results"][0][0]
        SOC_end = out["data"][1]["results"][0][0]
        delta_SOC = (SOC_end - SOC_ini) / SOC_ini

        out_dict = {
            "SOC_ini": SOC_ini,
            "SOC_end": SOC_end,
            "delta_SOC": delta_SOC
        }
        results.append((int(out["customId"]), out_dict))
        received_results += 1
        print ("received result: " + str(received_results) + " out of " + str(num_of_sims))

        if received_results == num_of_sims:
            leave = True

def set_param(params_list, p_name, p_value):
    #loop over params data structure
    for p, v in params_list.iteritems():
        if p == p_name:
            if isinstance(v, list):
                v[0] = p_value
            else:
                v = p_value

def seek_params(param_list, param_names, p_types, env):
    soilorg_params = env['params']['userSoilOrganicParameters']["DEFAULT"]
    crop_rotations = env["cropRotations"]

    #loop over sampled params
    for p_index, p_name in enumerate(param_names):
        p_value = params[p_index]
        p_type = p_types[p_index]
        #look in soilorg_params
        if p_type == "soil-organic":
            set_param(soilorg_params, p_name, p_value)
        else:
            #look in org fert and residue params
            for rot in crop_rotations:
                for cm in rot["cropRotation"]:
                    for ws in cm["worksteps"]:
                        if p_type == "org-fert" and ws["type"] == "OrganicFertilization":
                            set_param(ws["parameters"], p_name, p_value)
                        if p_type == "residue" and ws["type"] == "Sowing":
                            set_param(ws["crop"]["residueParams"], p_name, p_value)

#read params to test
p_names = []
p_bounds = []
p_types = []
with open(basepath + "/" + "SA_params.csv") as _:
    reader = csv.reader(_)
    next(reader, None) #skip header
    for row in reader:
        p_names.append(row[0])
        bounds = [float(row[1]), float(row[2])]
        p_bounds.append(bounds)
        p_types.append(row[4])

# DEFINE MODEL INPUTS
# problem (dict): The problem definition
# N (int): The number of samples to generate
# num_levels (int): The number of grid levels
# grid_jump (int): The grid jump size
# optimal_trajectories (int): The number of optimal trajectories to sample (between 2 and N)
# local_optimization (bool):
# Flag whether to use local optimization according to Ruano et al. (2012)
# Speeds up the process tremendously for bigger N and num_levels.
# Stating this variable to be true causes the function to ignore gurobi.

problem = {
    'num_vars': len(p_names),
    'names': p_names,
    'bounds': p_bounds,
    'groups': None
}

# SAMPLE
# morris.sample(problem, N, num_levels, grid_jump, # optimal_trajectories=None, local_optimization=False)
param_values = morris_pler.sample(problem, 10, 5, 1, optimal_trajectories=None, local_optimization=False)

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
        unique_combos.append(env_file)

#start connection and plug sockets
context = zmq.Context()
push_socket = context.socket(zmq.PUSH)
s_push = "tcp://" + config["server"]  + ":" + config["push-port"]
push_socket.connect(s_push)

pull_socket = context.socket(zmq.PULL)
s_pull = "tcp://" + config["server"]  + ":" + config["pull-port"]
pull_socket.connect(s_pull)

outfile = basepath + "/SA_results" + "_" + config["runs-file"]
with open(outfile, 'wb') as _:
    writer = csv.writer(_)
    header = ["sim_id", "morris_index"]
    for i in range(len(p_names)):
        ext_name = p_names[i] + "-" + p_types[i]
        header.append(ext_name)
    writer.writerow(header)

#read output instructions
with open(basepath + "/events.json") as _:
    events_json = json.load(_)

for env_file in unique_combos:
    print("starting SA with env " + str(env_file))
    #read proper env
    with open(envpath + "/dumped_envs/" + env_file) as _:
        env = json.load(_)
        #set path to climate
        path_0 = env["pathToClimateCSV"][0].split("data/")[1]
        path_1 = env["pathToClimateCSV"][1].split("data/")[1]
        if config["server"] == "localhost":
            env["pathToClimateCSV"][0] = paths["local-path"] + path_0
            env["pathToClimateCSV"][1] = paths["local-path"] + path_1
        else:
            env["pathToClimateCSV"][0] = paths["cluster-path"] + path_0
            env["pathToClimateCSV"][1] = paths["cluster-path"] + path_1
        #set events section
        env["events"] = events_json["events"]

    #clean results list (for collector)
    results = []
    
    #clean array for result analysis
    Y = np.empty([param_values.shape[0]])

    #launch parallel thread for the collector
    collector = FuncThread(collect_results, param_values.shape[0])
    collector.daemon = True
    collector.start()

    for i, params in enumerate(param_values):
        env["customId"] = str(i)

        seek_params(params, problem["names"], p_types, env)

        push_socket.send_json(env)
        
        #with open(basepath + "/dumped-" + env_file, "w") as _:
        #    _.write(json.dumps(env, indent=4))
        #    print("dumped env: " + env_file)

        print "sent " + str(i + 1) + " out of " + str(len(param_values))


    #wait until the collector finishes
    collector.join()

    results.sort(key=lambda tup: tup[0])

    for j in range(len(results)):
        Y[j] = results[j][1]["delta_SOC"]

    # ANALYZE
    # SALib.analyze.morris.analyze(problem, X, Y, num_resamples=1000, conf_level=0.95, print_to_console=False, grid_jump=2, num_levels=4)
    # problem (dict): The problem definition
    # X (numpy.matrix): The NumPy matrix containing the model inputs
    # Y (numpy.array): The NumPy array containing the model outputs
    # num_resamples (int): The number of resamples used to compute the confidence intervals (default 1000)
    # conf_level (float): The confidence interval level (default 0.95)
    # print_to_console (bool): Print results directly to console (default False)
    # grid_jump (int): The grid jump size, must be identical to the value passed to SALib.sample.morris.sample() (default 2)
    # num_levels (int): The number of grid levels, must be identical to the value passed to SALib.sample.morris (default 4)

    morris_indices = morris_lyze.analyze(problem, param_values, Y, num_resamples=100, conf_level=0.95, print_to_console=False, grid_jump=1, num_levels=5)

    with open(outfile, 'ab') as _:
        writer = csv.writer(_)
        row_mu = [env_file.replace(".json", ""), "mu_star"]
        row_sigma = [env_file.replace(".json", ""), "sigma"]
        
        for i, par in enumerate(morris_indices['names']):
            row_mu.append(morris_indices['mu_star'][i])
            row_sigma.append(morris_indices['sigma'][i])

        writer.writerow(row_mu)
        writer.writerow(row_sigma)
    
    if config["write-all-out"]:
        fname = basepath + "/out-runs/" + env_file.replace(".json", ".csv")
        with open(fname, "wb") as _:
            writer = csv.writer(_, delimiter=",")
            header = ["run_id", "SOCini", "SOCend", "DeltaOC"]
            for i in range(len(p_names)):
                ext_name = p_names[i] + "-" + p_types[i]
                header.append(ext_name)
            writer.writerow(header)

            for i, params in enumerate(param_values):
                row =[]
                row.append(i)
                row.append(results[i][1]["SOC_ini"])
                row.append(results[i][1]["SOC_end"])
                row.append(results[i][1]["delta_SOC"])
                for p in params:
                    row.append(p)
                writer.writerow(row)


print("finished!")

