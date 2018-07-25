from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import numpy as np
import spotpy
import MONICA_adapter
import re

class spot_setup(object):
    def __init__(self, user_params, obslist, config, env):        
        self.params = []
        for par in user_params:
            self.params.append(spotpy.parameter.Uniform(par["name"],
                                                        par["low"],
                                                        par["high"],
                                                        par["stepsize"],
                                                        par["optguess"],
                                                        par["minbound"],
                                                        par["maxbound"]))
        self.monica_model = MONICA_adapter.monica_adapter(obslist, config, env)

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        simulations = self.monica_model._run(vector)
        return simulations

    def evaluation(self):
        return self.monica_model.observations

    def objectivefunction(self,simulation,evaluation):
        objectivefunction= -spotpy.objectivefunctions.rmse(evaluation,simulation)
        return objectivefunction
