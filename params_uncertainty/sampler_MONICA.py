from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import spotpy
import spotpy_setup_MONICA
import csv
import helper_functions as helper

config = {
    "server": "cluster1", #"localhost"
    "push-port": "6666",
    "pull-port": "7777"
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
        "runexps": None # [13,25] #None: all
    }
}
run = "muencheberg"

exp_maps = helper.read_exp_map(filename=datasets[run]["map_file"], exps=datasets[run]["runexps"])
observations = helper.read_obs(filename=datasets[run]["observations"], exps=datasets[run]["runexps"])
params = helper.read_params(datasets[run]["params"])

spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, observations, config, distrib="normal")


sampler = spotpy.algorithms.demcz(spot_setup, dbname='DEMCz', dbformat='csv')
sampler.sample(200)
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

#test_plot_params = [0.092149442151153976, 0.00014319894113422492, 0.57139534158491667, 0.57521733905356565, 0.00026468815754662268, 5.8581353535972362e-05, 0.33324556113900239]
#simulations = spot_setup.simulation(test_plot_params, True)

#plot results
print("preparing charts...")
helper.SOC_plot(observations, simulations, filename="SOC_muench.png")

print("finished!")



