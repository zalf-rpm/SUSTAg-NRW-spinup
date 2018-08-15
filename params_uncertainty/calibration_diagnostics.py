import spotpy
import numpy as np
import spotpy_setup_MONICA
import helper_functions as helper
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import seaborn as sns
import csv


def generate_random_sample(spot_setup, n=1000):
    randompar=spot_setup.parameters()['random']
    for i in range(n):
        randompar=np.column_stack((randompar,spot_setup.parameters()['random']))
    return randompar

def find_min_max(spot_setup):
    randompar = generate_random_sample(spot_setup)
    return np.amin(randompar,axis=1),np.amax(randompar,axis=1)


def distrib_plot_default(results, spot_setup, min_vs, max_vs, filename):
    fig = plt.figure(figsize=(16, 20))

    plot_n = 1
    for p in range(len(spot_setup.parameters())):
        #iterations plot
        plt.subplot(len(spot_setup.parameters()),2, plot_n)
        x = results['par'+str(spot_setup.parameters()['name'][p])]
        for i in range(int(max(results['chain']))):
            index=np.where(results['chain']==i)
            plt.plot(x[index],'.')
        plt.title(str(spot_setup.parameters()['name'][p]))
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
        plt.title(str(spot_setup.parameters()['name'][p]))
        plt.xlim(min_vs[p],max_vs[p])
        if p == len(spot_setup.parameters()) - 1:
            plt.xlabel('Parameter range')
        plot_n += 1
    
    plt.subplots_adjust(hspace=0.5)
    #plt.show()

    fig.savefig(filename,dpi=300)
    print(filename + " saved!")

def distrib_plot_prior_post(spot_setup, posterior, min_vs, max_vs, filename):
    fig = plt.figure()
    nparams = len(spot_setup.parameters())
    nrows = int(round((nparams + 1) / 2))
    ncols = 2
    index = 1

    for p in range(nparams):
        p_name = "" + spot_setup.parameters()['name'][p]
        my_ax = fig.add_subplot(nrows, ncols, index)
        my_ax.set_title(p_name)
        my_ax.set_xlim(min_vs[p],max_vs[p])

        #prior distribution
        mu = spot_setup.user_params[p]["avg"]
        sigma = spot_setup.user_params[p]["st_dev"]
        x = np.linspace(mu - 3*sigma, mu + 3*sigma, 100)
        my_ax.plot(x, mlab.normpdf(x, mu, sigma))

        #posterior distribution
        sns.distplot(posterior['par' + p_name], hist=False, ax=my_ax)
        
        index += 1

    fig.savefig(filename, dpi=600)
    print(filename + " saved!")
        
        

#Analysis & plotting
results = spotpy.analyser.load_csv_results('DEMCz_1000')

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

posterior=spotpy.analyser.get_posterior(results, percentage=10)

min_vs,max_vs = find_min_max(spot_setup)
#distrib_plot_prior_post(spot_setup, posterior, min_vs, max_vs, "prior_post.png")

#spotpy.analyser.plot_parameterInteraction(posterior)
#distrib_plot_default(results, spot_setup, min_vs, max_vs, 'distrib_parameters_allruns.png')
#distrib_plot_default(posterior, spot_setup, min_vs, max_vs, 'distrib_parameters_posterior.png')

#draw samples to decide minimum representative sample size
#sample_sizes = range(5,30,5)
sample_sizes = [15]

for size in sample_sizes:
    s_ids = np.random.choice(100, size, replace=False)
    sample = posterior[s_ids]
    np.savetxt("sample_" + str(size) + ".csv", sample, delimiter=",")
    distrib_plot_default(sample, spot_setup, min_vs, max_vs, 'distrib_parameters_size_' + str(size) + '.png')

print "finished"