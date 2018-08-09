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

    def _run(self, vector):

        self.evallist = []

        #set params according to spotpy sampling
        soil_profile_params = self.env["params"]["siteParameters"]["SoilProfileParameters"]
        for i in range(len(vector)):
            soil_profile_params[i]["SoilOrganicCarbon"][0] = round(vector[i], 3)
        
        #send job to the MONICA server
        self.socket_push.send_json(self.env)

        #collect results
        leave = False
        while not leave:
            try:
                rec_msg = self.socket_pull.recv_json()
            except:
                continue

            for res in rec_msg["data"]:
                try:
                    self.evallist.append(res["results"][0][0])
                except:
                    print("error in results for id " + rec_msg["customId"])
            leave = True

        #return the evaluation list for spotpy
        return self.evallist
    
    def close_sockets(self):
        self.socket_push.close()
        self.socket_pull.close()
        self.context.term()
