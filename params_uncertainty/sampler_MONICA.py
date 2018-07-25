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
import matplotlib.pyplot as plt
from matplotlib import colors
from collections import defaultdict
import helper_functions

font = {'family' : 'calibri',
    'weight' : 'normal',
    'size'   : 18}

def produce_plot(experiments, variable, ylabel='Best model simulation', xlabel='Date'):    
    #cnames = list(colors.cnames)

    plt.rc('font', **font)
    colors = ['grey', 'black', 'brown', 'red', 'orange', 'yellow', 'green', 'blue']
    n_subplots = max(2, len(experiments))
    # N subplots, sharing x axis
    width = 20
    height = n_subplots * 10
    f, axarr = plt.subplots(n_subplots, sharex=False, figsize=(width, height))
    i=0
    for exp in experiments:
        RMSE = spotpy.objectivefunctions.rmse(experiments[exp]["obs"], experiments[exp]["sims"])
        axarr[i].plot(experiments[exp]["dates"], experiments[exp]["obs"], 'ro', markersize=8, label='obs data')
        #axarr[i].plot(experiments[exp]["dates"], experiments[exp]["sims"],'-', color=colors[7], linewidth=2, label='exp ' + exp + ': RMSE=' + str(round(RMSE, 2)))
        axarr[i].plot(experiments[exp]["all_dates"], experiments[exp]["daily"],'-', color=colors[7], linewidth=2, label='exp ' + exp + ': RMSE=' + str(round(RMSE, 3)))
        axarr[i].legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
        #axarr[i].set_title(str(exp))
        i +=1
    filename = variable + '.png'
    f.savefig(filename)
    text = 'A figure has been saved as ' + filename
    print(text)


exp_maps= helper_functions.read_exp_map()
obslist = helper_functions.read_obs()
#order obslist by exp_id to avoid mismatch between observation/evaluation lists
obslist = sorted(obslist, key=helper_functions.getKey)
params = helper_functions.read_params()

spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, obslist, distrib="normal")
rep = 500
results = []

sampler = spotpy.algorithms.demcz(spot_setup, dbname='DEMCz', dbformat='csv')
sampler.sample(rep)

#sampler = spotpy.algorithms.sceua(spot_setup, dbname='SCEUA', dbformat='csv')
#sampler.sample(rep, ngs=len(params)+1, kstop=50)

results.append(sampler.getdata())

best = sampler.status.params

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

#PLOTTING
#get the best model run
for i in range(len(results)):
    index,maximum=spotpy.analyser.get_maxlikeindex(results[i])

bestmodelrun=list(spotpy.analyser.get_modelruns(results[i])[index][0]) #Transform values into list to ensure plotting

obs_dates = spot_setup.evaluation(get_dates_dict=True)
obs_values = spot_setup.evaluation(get_values_dict=True)

#Run with optimized params
print("running simulations with optimized params")
spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, obslist, True)
daily_out = spot_setup.simulation(best, True)

#retrieve info for plots
print("preparing charts...")
for variable in obs_dates:
    exps = {}
    for experiment in obs_dates[variable]:
        sims = []
        obs = []
        dates = []
        for k,v in obs_dates[variable][experiment]:
            sims.append(bestmodelrun[k])
            dates.append(v)
        for k,v in obs_values[variable][experiment]:
            obs.append(v)
        exps[experiment] = {}
        exps[experiment]["dates"] = dates
        exps[experiment]["sims"] = sims
        exps[experiment]["obs"] = obs
        exps[experiment]["daily"] = daily_out[int(experiment)][variable]
        exps[experiment]["all_dates"] = daily_out[int(experiment)]["Date"]
    produce_plot(exps,variable)

print("finished!")



