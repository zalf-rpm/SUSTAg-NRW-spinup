import json
import monica_io
import zmq
import os
from datetime import date
import collections
import threading
from threading import Thread
from collections import defaultdict
import helper_functions as helper


class monica_adapter(object):
    def __init__(self, exp_maps, observations, config, finalrun):        
        #read params - they will be modified in the run method
        with open(config["soilorg_params_path"]) as _:
            self.soil_organic_params = json.load(_)
        
        with open(config["fertorg_params_path"]) as _:
            self.fert_organic_params = json.load(_)
            #following params were calibrated using exps 25 and 35 from V140 Muencheberg
            self.fert_organic_params["PartAOM_to_AOM_Fast"][0] = 0.81
            self.fert_organic_params["PartAOM_to_AOM_Slow"][0] = 0.19
        
        #observations data structures
        self.observations = observations
        self.obslist = helper.create_spotpylist(self.observations)

        #create envs
        self.envs = []
        for exp_map in exp_maps:
            with open(exp_map["sim_file"]) as simfile:
                sim = json.load(simfile)
                sim["crop.json"] = exp_map["crop_file"]
                sim["site.json"] = exp_map["site_file"]
                sim["climate.csv"] = exp_map["climate_file"]

            with open(exp_map["site_file"]) as sitefile:
                site = json.load(sitefile)

            with open(exp_map["crop_file"]) as cropfile:
                crop = json.load(cropfile)
                crop["cropRotations"] = []

            env = monica_io.create_env_json_from_json_config({
                "crop": crop,
                "site": site,
                "sim": sim
            })

            #create references to self.soil_organic_params and self.fert_organic_params
            env["params"]["userSoilOrganicParameters"] = self.soil_organic_params
            for cm in env["cropRotation"]:
                for ws in cm["worksteps"]:
                    if ws["type"] == "OrganicFertilization":
                        ws["parameters"] = self.fert_organic_params


            #customize events section
            env["events"] = []
            with open(config["events-file"]) as _:
                json_out = json.load(_)
                env["events"] = json_out["events"]


            #climate is read by the server
            env["csvViaHeaderOptions"] = sim["climate.csv-options"]
            env["pathToClimateCSV"] = []
            if config["server"] == "localhost":
                env["pathToClimateCSV"].append(sim["climate.csv"])
            else:
                local_path_to_climate = os.path.dirname(os.path.abspath(__file__)) + "\\climate_files"
                cluster_path_to_climate = "/archiv-daten/md/projects/sustag/climate_files_uncertainty_analysis"
                env["pathToClimateCSV"].append(sim["climate.csv"].replace(local_path_to_climate, cluster_path_to_climate).replace("\\", "/"))
                #print env["pathToClimateCSV"][0]

            env["customId"] = str(exp_map["treatment"]) + "|" + str(exp_map["parcel"]) + "|"  + config["events-file"]

            self.envs.append(env)

        #open sockets
        self.context = zmq.Context()
        self.socket_push = self.context.socket(zmq.PUSH)
        s_push = "tcp://" + config["server"]  + ":" + config["push-port"]
        self.socket_push.connect(s_push)

        self.socket_pull = self.context.socket(zmq.PULL)
        s_pull = "tcp://" + config["server"]  + ":" + config["pull-port"]
        self.socket_pull.connect(s_pull)

    def run(self, args):
        return self._run(*args)

    def _run(self, vector, user_params, finalrun):

        self.simulations = helper.common_datastructure()

        #set params according to spotpy sampling
        def set_param(params_list, p_name, p_value):
            #check nested structure
            add_index = False
            if isinstance(params_list[p_name], list) and isinstance(params_list[p_name][1], basestring):
                add_index = True
            #set val
            if not add_index:
                params_list[p_name] = p_value
            else:
                params_list[p_name][0] = p_value

        for i in range(len(user_params)):
            p_name = user_params[i]["name"]
            p_type = user_params[i]["type"]
            p_value = vector[i]
            if p_type == "soil":
                set_param(self.soil_organic_params, p_name, p_value)
            elif p_type == "fert":
                set_param(self.fert_organic_params, p_name, p_value)
                #change derived params
                if p_name == "PartAOM_to_AOM_Fast":
                    set_param(self.fert_organic_params, "PartAOM_to_AOM_Slow", (1 - p_value))

        #launch parallel thread for the collector
        collector = Thread(target=self.collect_results)
        collector.daemon = True
        collector.start()

        #send jobs to the cluster
        for env in self.envs:
            self.socket_push.send_json(env)
            #print("sent custom ID: " + env["customId"])

        #wait until the collector finishes
        collector.join()
        
        if not finalrun:
            #build the evaluation list for spotpy
            out = helper.create_spotpylist(self.observations, self.simulations)
        else:
            #return complete outputs
            out = self.simulations
        
        return out

    def collect_results(self):
        received_results = 0
        leave = False
        while not leave:
            try:
                rec_msg = self.socket_pull.recv_json()
            except:
                continue
            
            custom_id = rec_msg["customId"].split("|")
            treatment = int(custom_id[0])
            parcel = int(custom_id[1])
            events_file = custom_id[2]
            years = rec_msg["data"][0]["results"][0]

            if events_file == "template_events_SOC.json":
                #normal case (calibration)                
                vals = rec_msg["data"][0]["results"][1]

                for i in range(len(years)):
                    yr = years[i]
                    self.simulations[treatment][yr]["parcel"].append(parcel)
                    self.simulations[treatment][yr]["data"].append(vals[i] * 100)
                    #!!!!!! * 100 converts kg kg-1 to %
            
            elif events_file == "template_events_SOC_fractions.json":
                #additional output to evaluate spinup
                for res_id in range(len(rec_msg["data"][0]["results"])):
                    if res_id==0:
                        #skip "years"
                        continue
                    out_name = rec_msg["data"][0]["outputIds"][res_id]["name"]
                    vals = rec_msg["data"][0]["results"][res_id]
                    for i in range(len(years)):
                        yr = years[i]
                        self.simulations[treatment][yr][out_name].append(vals[i])

            received_results += 1

            #print("total received: " + str(received_results))

            if received_results == len(self.envs):
                leave = True
