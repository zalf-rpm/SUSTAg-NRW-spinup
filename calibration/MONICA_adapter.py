import json
import sys
import monica_io
import zmq
import csv
import os
from datetime import date
import collections
import threading
from threading import Thread
from collections import defaultdict


class monica_adapter(object):
    def __init__(self, obslist, config, env):

        self.observations = obslist #for spotpy
        self.env = env
        self.config = config

        self.context = zmq.Context()
        self.socket_push = self.context.socket(zmq.PUSH)
        s_push = "tcp://" + self.config["server"]  + ":" + self.config["push-port"]
        self.socket_push.connect(s_push)

    def run(self, args):
        return self._run(*args)

    def _run(self, vector):

        self.evallist = []

        #set params according to spotpy sampling
        soil_profile_params = self.env["params"]["siteParameters"]["SoilProfileParameters"]
        for i in range(len(vector)):
            soil_profile_params[i]["SoilOrganicCarbon"][0] = round(vector[i], 3)

        #launch parallel thread for the collector
        collector = Thread(target=self.collect_results)
        collector.daemon = True
        collector.start()

        #send jobs to the MONICA server
        self.socket_push.send_json(self.env)

        #wait until the collector finishes
        collector.join()

        #return the evaluation list for spotpy
        return self.evallist

        
    def collect_results(self):
        socket_pull = self.context.socket(zmq.PULL)
        s_pull = "tcp://" + self.config["server"]  + ":" + self.config["pull-port"]
        socket_pull.connect(s_pull)
        
        leave = False
        while not leave:
            try:
                rec_msg = socket_pull.recv_json()
            except:
                continue
            
            for res in rec_msg["data"]:
                try:
                    self.evallist.append(res["results"][0][0])
                except:
                    print("error in results for id " + rec_msg["customId"])
            leave = True
