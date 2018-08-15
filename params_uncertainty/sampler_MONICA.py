from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import spotpy
import spotpy_setup_MONICA
import csv
import helper_functions as helper

config = {
    "server": "cluster1", #"cluster1", "localhost"
    "push-port": "6666",
    "pull-port": "7777",
    "events-file": "template_events_SOC.json",
    "soilorg_params_path": "C:/Users/stella/Documents/GitHub/monica-parameters/general/soil-organic.json",
    "fertorg_params_path": "C:/Users/stella/Documents/GitHub/monica-parameters/organic-fertilisers/CAM.json"
}

datasets = {
    "c_mip_deprecated": {
        "map_file": "crop_sim_site_MAP_cmip.csv",
        "observations": "observations_cmip.csv",
        "params": "calibratethese.csv",
        "runexps": [1] # 1: Askov, 3: Grignon, 4: Kursk, 5: Rothamsted, 6: Ultuna, 7: Versailles, None: all
    },
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

sampler = spotpy.algorithms.demcz(spot_setup, dbname='DEMCz', dbformat='csv')
sampler.sample(1000)
best = sampler.status.params

with open('optimizedparams.csv', 'wb') as outcsvfile:
    writer = csv.writer(outcsvfile)
    for i in range(len(best)):
        outrow=[]
        outrow.append(params[i]["name"])
        outrow.append(best[i])
        writer.writerow(outrow)
    text='optimized parameters saved!'
    print(text)

#Run with optimized params
print("running simulations with optimized params")
simulations = spot_setup.simulation(best, True)

#opt_params = [0.280363071077091, 0.000133212927255853, 0.537688327637781, 0.671181424873132, 0.000336789789134262, 0.0000603273664091705, 0.607108557360137]
#simulations = spot_setup.simulation(opt_params, True)

#plot results
print("preparing charts...")
#helper.SOC_fractions_plot(simulations, filename="SOC_fractions_muench_new.png") #change template events, skip calibration and run with opt params to use this
helper.SOC_plot(observations, simulations, filename="SOC_muench_1000.png")

print("finished!")



