import spotpy
import numpy as np
import spotpy_setup_MONICA
import helper_functions as helper
import pylab as plt


def find_min_max(spot_setup):
    randompar=spot_setup.parameters()['random']
    for i in range(1000):
        randompar=np.column_stack((randompar,spot_setup.parameters()['random']))
    return np.amin(randompar,axis=1),np.amax(randompar,axis=1)


def distrib_plot(results, spot_setup, min_vs, max_vs):
    fig= plt.figure(figsize=(16,16))

    plot_n = 1
    for p in range(len(spot_setup.parameters())):
        #iterations plot
        plt.subplot(len(spot_setup.parameters()),2, plot_n)
        x = results['par'+str(spot_setup.parameters()['name'][p])]
        for i in range(int(max(results['chain']))):
            index=np.where(results['chain']==i)
            plt.plot(x[index],'.')
        plt.ylabel(str(spot_setup.parameters()['name'][p]))
        plt.ylim(min_vs[p],max_vs[p])
        if p == len(spot_setup.parameters()) - 1:
            plt.xlabel('Iterations')
        plot_n += 1

        #distribution plot
        plt.subplot(len(spot_setup.parameters()),2,plot_n)
        x = results['par'+spot_setup.parameters()['name'][p]]
        normed_value = 1
        hist, bins = np.histogram(x, bins=10, density=False)
        widths = np.diff(bins)
        hist *= normed_value
        plt.bar(bins[:-1], hist, widths)
        plt.ylabel(str(spot_setup.parameters()['name'][p]))
        plt.xlim(min_vs[p],max_vs[p])
        if p == len(spot_setup.parameters()) - 1:
            plt.xlabel('Parameter range')
        plot_n += 1

    plt.show()

    fig.savefig('distrib_parameters.png',dpi=300)
    print("distrib_parameters.png saved!")


#Analysis & plotting
results = spotpy.analyser.load_csv_results('DEMCz')

datasets = {
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

spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, observations, config=None, distrib="normal")

posterior=spotpy.analyser.get_posterior(results, percentage=20)
#spotpy.analyser.plot_parameterInteraction(posterior)
#print("plotted posterior")

min_vs,max_vs = find_min_max(spot_setup)
#distrib_plot(results, spot_setup, min_vs, max_vs)
distrib_plot(posterior, spot_setup, min_vs, max_vs)

print "finished"