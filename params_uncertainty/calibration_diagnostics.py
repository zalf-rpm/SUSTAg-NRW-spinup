import spotpy
import numpy as np
import spotpy_setup_MONICA
import helper_functions
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
results = spotpy.analyser.load_csv_results('SCEUA')

exp_maps = helper_functions.read_exp_map()
obslist = helper_functions.read_obs()
obslist = sorted(obslist, key=helper_functions.getKey)
params = helper_functions.read_params()

spot_setup = spotpy_setup_MONICA.spot_setup(params, exp_maps, obslist)

posterior=spotpy.analyser.get_posterior(results)
#spotpy.analyser.plot_parameterInteraction(posterior)
#print("plotted posterior")

min_vs,max_vs = find_min_max(spot_setup)
distrib_plot(results, spot_setup, min_vs, max_vs)
distrib_plot(posterior, spot_setup, min_vs, max_vs)

#min_vs,max_vs = find_min_max(spot_setup)
#distrib_plot(results, spot_setup, min_vs, max_vs)

print "finished"