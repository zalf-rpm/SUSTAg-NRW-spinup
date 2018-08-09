from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import numpy as np
import spotpy
import MONICA_adapter
import re

class spot_setup(object):
    def __init__(self, user_params, exp_maps, observations, config, distrib="uniform", finalrun=False):
        self.user_params = user_params
        self.params = []
        for par in user_params:
            parname = par["name"] + "_" + par["type"] 
            if distrib.lower() == "uniform":
                self.params.append(spotpy.parameter.Uniform(name=parname, low=par["low"], high=par["high"], optguess=par["optguess"]))
            elif distrib.lower() == "normal":
                #self.params.append(spotpy.parameter.Normal(name=parname, mean=par["avg"], stddev=par["st_dev"], optguess=par["optguess"], minbound=max(par["low"], 0.000001), maxbound=par["high"]))
                self.params.append(spotpy.parameter.Normal(name=parname, mean=par["avg"], stddev=par["st_dev"], optguess=par["optguess"], minbound=par["low"], maxbound=par["high"]))
        self.monica_model = MONICA_adapter.monica_adapter(exp_maps, observations, config, finalrun)

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector, finalrun=False):
        #the vector comes from spotpy, self.user_params holds the information coming from csv file
        simulations= self.monica_model._run(vector, self.user_params, finalrun)
        return simulations

    def evaluation(self):
        return self.monica_model.obslist

    def objectivefunction(self,simulation,evaluation):
        objectivefunction= -spotpy.objectivefunctions.rmse(evaluation,simulation)
        return objectivefunction
