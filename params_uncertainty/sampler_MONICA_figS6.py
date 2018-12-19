from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import spotpy
import spotpy_setup_MONICA
import csv
import helper_functions as helper

#1. get best 10% parameters
results = spotpy.analyser.load_csv_results('DEMCz_1000')
posterior=spotpy.analyser.get_posterior(results, percentage=10)

config = {
    "server": "cluster1", #"cluster1", "localhost"
    "push-port": "6666",
    "pull-port": "7777",
    "events-file": "template_events_SOC.json",
    "soilorg_params_path": "C:/Users/stella/Documents/GitHub/monica-parameters/general/soil-organic.json",
    "fertorg_params_path": "C:/Users/stella/Documents/GitHub/monica-parameters/organic-fertilisers/CAM.json"
}

datasets = {
    "muencheberg": {
        "map_file": "crop_sim_site_MAP_mue.csv",
        "observations": "observations_mue.csv",
        "params": "calibratethese.csv",
        "runexps": None #[13,25] #None: all
    }
}
run = "muencheberg"

exp_maps = helper.read_exp_map(filename=datasets[run]["map_file"], exps=datasets[run]["runexps"])
observations = helper.read_obs(filename=datasets[run]["observations"], exps=datasets[run]["runexps"])
params = helper.read_params(datasets[run]["params"])

spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, observations, config, distrib="normal")

outfile = "test_figS6/S6_Rdata.csv"
with open(outfile, "wb") as _:
    writer = csv.writer(_)
    header = ["treatment", "parcel", "year", "sim_SOC", "obs_SOC", "like", "p_counter"]
    writer.writerow(header)

counter = 0
for row in posterior:
    counter += 1
    p_set = []
    like = row[0]
    #QandD: parameters are stored from [1] to [7] in each row
    for i in range(1, 8):
        p_set.append(row[i])
    print("running parameter set " + str(counter) + " of " + str(len(posterior)))
    simulations = spot_setup.simulation(p_set, True)
    #print("preparing charts...")
    #helper.SOC_plot(observations, simulations, filename="test_figS6/SOC_muench_1000.png")

    #append data to outfile
    with open(outfile, "ab") as _:
        writer = csv.writer(_)
        for tr in simulations.keys():
            for yr in simulations[tr].keys():
                for i in range(len(simulations[tr][yr]["parcel"])):
                    parcel = simulations[tr][yr]["parcel"][i]
                    sim_val = simulations[tr][yr]["data"][i]
                    obs_val = "NA"
                    if yr in observations[tr].keys():
                        if i < len(observations[tr][yr]["parcels"]): #!!observations dict is initialized with "parcels", simulations with "parcel"
                            obs_val = observations[tr][yr]["data"][i]
                    outrow = []
                    outrow.append(tr)
                    outrow.append(parcel)
                    outrow.append(yr)
                    outrow.append(sim_val)
                    outrow.append(obs_val)
                    outrow.append(like)
                    outrow.append(counter)
                    writer.writerow(outrow)
print("finished!")
    
'''
#Run with best params
print("running simulations with optimized params")
simulations = spot_setup.simulation(best, True)

#opt_params = [0.280363071077091, 0.000133212927255853, 0.537688327637781, 0.671181424873132, 0.000336789789134262, 0.0000603273664091705, 0.607108557360137]
#simulations = spot_setup.simulation(opt_params, True)

#plot results
print("preparing charts...")
#helper.SOC_fractions_plot(simulations, filename="SOC_fractions_muench_new.png") #change template events, skip calibration and run with opt params to use this
helper.SOC_plot(observations, simulations, filename="test_figS6/SOC_muench_1000.png")

print("finished!")
'''



