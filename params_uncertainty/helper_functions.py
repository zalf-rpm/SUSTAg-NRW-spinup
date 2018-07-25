import os
import csv
from datetime import date

def make_lambda(excel):
    return lambda v, p: eval(excel)

def read_exp_map():
    exp_maps = []
    basepath = os.path.dirname(os.path.abspath(__file__))
    with open('crop_sim_site_MAP.csv') as exp_mapfile:
        dialect = csv.Sniffer().sniff(exp_mapfile.read(), delimiters=';,\t')
        exp_mapfile.seek(0)
        reader = csv.reader(exp_mapfile, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            exp_map = {}
            exp_map["exp_ID"] = row[0]
            exp_map["sim_file"] = basepath+"\\sim_files\\"+row[1]
            exp_map["crop_file"] = basepath+"\\crop_files\\"+row[2]
            exp_map["site_file"] = basepath+"\\site_files\\"+row[3]
            exp_map["climate_file"] = basepath+"\\climate_files\\"+row[4]
            exp_map["species_file"] = basepath+"\\param_files\\"+row[5]
            exp_map["cultivar_file"] = basepath+"\\param_files\\"+row[6]
            exp_map["where_in_rotation"] = [int(x) for x in row[7].split("-")]
            exp_map["crop_ID"] = row[8]
            exp_map["soil_organic_file"] = basepath+"\\param_files\\"+row[9]
            exp_maps.append(exp_map)
    return exp_maps

#read observations
def read_obs():
    obslist = [] #for envs (outputs)
    with open('observations.csv') as obsfile:
        dialect = csv.Sniffer().sniff(obsfile.read(), delimiters=';,\t')
        obsfile.seek(0)
        reader = csv.reader(obsfile, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            if row[6].upper() == "Y":
                record = {}
                record["exp_ID"] = row[0]
                record["date"] = date(int(row[1]), int(row[2]), int(row[3])).isoformat()
                record["variable"] = row[4]
                record["value"] = float(row[5])
                if row[8] != "": #layer aggregation
                    aggregation = []
                    nested_arr=[]
                    nested_arr.append(int(row[8]))
                    nested_arr.append(int(row[9]))
                    nested_arr.append(unicode(row[10].upper()))
                    aggregation.append(unicode(row[4]))               
                    aggregation.append(nested_arr)
                    record["aggregation"] = aggregation
                if row[11] != "": #temporal aggregation
                    t_aggr_dict = {}
                    t_aggr_dict[unicode("from")] = unicode(date(int(row[11]), int(row[12]), int(row[13])).isoformat())
                    t_aggr_dict[unicode("to")] = unicode(date(int(row[14]), int(row[15]), int(row[16])).isoformat())
                    record["time_aggr"] = t_aggr_dict
                    outer_arr = []
                    nested_arr=[]
                    nested_arr.append(unicode(row[4]))
                    nested_arr.append(unicode(row[17].upper()))
                    outer_arr.append(nested_arr)
                    record["time_aggr_var"] = outer_arr
                obslist.append(record)
    return obslist

#order obslist by exp_id to avoid mismatch between observation/evaluation lists
def getKey(record):
    return int(record["exp_ID"])

#read params to be calibrated
def read_params():
    params = []
    with open('calibratethese.csv') as paramscsv:
        dialect = csv.Sniffer().sniff(paramscsv.read(), delimiters=';,\t')
        paramscsv.seek(0)
        reader = csv.reader(paramscsv, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            p={}
            p["name"] = row[0]
            p["array"] = row[1]
            p["low"] = float(row[2])
            p["high"] = float(row[3])
            p["optguess"] = float(row[4])
            p["avg"] = float(row[5])
            p["st_dev"] = float(row[6])
            if len(row) == 8 and row[7] != "":
                p["derive_function"] = make_lambda(row[8])
            params.append(p)
    return params