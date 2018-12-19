#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
# Tommaso Stella <tommaso.stella@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import sys
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
#print sys.path

#import ascii_io
#import json
import csv
import types
import os
from datetime import datetime
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
import re

LOCAL_RUN = False
SOC_STUDY = False #if true, run only unique cells with multiple parameters sets
SAVE_TO_EXTERNAL_DRIVE = False
ext_drive_path = "E:/sustag_out/"

def deltaSOC_perc(SOCini, SOCend):
    deltaSOC = 0
    if SOCini > 0:
        deltaSOC = (SOCend - SOCini) / SOCini * 100
    return deltaSOC

def retuned_exported_residues(agb, primary_yield, secondary_yield, use_secondary_yields, rootCrop):
    return_residues = agb
    if not rootCrop:
        return_residues -= primary_yield
    export_residues = 0

    if use_secondary_yields:
        return_residues -= secondary_yield
        if return_residues < 0.0001 and return_residues > -0.5:
            return_residues = 0
        export_residues += secondary_yield

    return return_residues, export_residues

def is_root_crop(cp):
    out = False
    root_cps = ["potato", "sugar beet"]
    for root_cp in root_cps:
        if root_cp in cp:
            out = True
            break
    return out

def skip(harvest_date, last_year):
    skip_it = False
    harvest_date = datetime.strptime(harvest_date, "%Y-%m-%d")
    if harvest_date == datetime(last_year, 12, 31):
        #the crop is harvested early due to the end of the simulation period,
        #values of this crop should be ignored.
        skip_it = True
    return skip_it

def cyclelength(sowing, harvest):
    sowing = datetime.strptime(sowing, "%Y-%m-%d")
    harvest = datetime.strptime(harvest, "%Y-%m-%d")
    return (harvest - sowing).days

def create_year_output(oids, row, col, rotation, prod_level, values, start_recording_out, KA5_txt, soil_type, main_cp_iteration):
    "create year output lines"
    row_col = "{}{:03d}".format(row, col)
    out = []
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                if isinstance(val, types.ListType):
                    for val_ in val:
                        vals[oid["displayName"]] = val_
                else:
                    vals[oid["displayName"]] = val

            if vals.get("Year", 0) >= start_recording_out:
                out.append([
                    row_col,
                    rotation,
                    prod_level,
                    vals.get("Year", "NA"),
                    deltaSOC_perc(vals.get("SOCini", 0), vals.get("SOCend", 0)),
                    vals.get("SOCavg", "NA"),
                    vals.get("Rh", "NA"),
                    vals.get("NEP", "NA"),
                    vals.get("Act_ET", "NA"),
                    vals.get("Act_Ev", "NA"),
                    vals.get("PercolationRate", "NA"),
                    #vals.get("Irrig", "NA"),
                    vals.get("NLeach", "NA"),
                    vals.get("ActNup", "NA"),
                    vals.get("NFert", "NA"),
                    vals.get("NOrgFert", "NA"),
                    vals.get("N2O", "NA"),
                    vals.get("Precip", "NA"),
                    vals.get("Tavg", "NA"),
                    KA5_txt,
                    soil_type
                    #main_cp_iteration
                    #vals.get("Clay", "NA"),
                    #vals.get("Silt", "NA"),
                    #vals.get("Sand", "NA")
                ])

    return out


def create_crop_output(oids, row, col, rotation, prod_level, values, use_secondary_yields, start_recording_out, residue_humus_balance, main_cp_iteration):
    "create crop output lines"
    row_col = "{}{:03d}".format(row, col)
    out = []
    last_year = values[0][-1]
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                if isinstance(val, types.ListType):
                    for val_ in val:
                        vals[oid["displayName"]] = val_
                else:
                    vals[oid["displayName"]] = val

            rootCrop =  is_root_crop(vals["Crop"])
            if residue_humus_balance and vals.get("Year", 0) >= 2003:
                return_residues = vals.get("optCarbonReturnedResidues", "NA")
                export_residues = vals.get("optCarbonExportedResidues", "NA")
            else:
                return_residues, export_residues = retuned_exported_residues(float(vals["AbBiom"]), float(vals["Yield"]), float(vals["SecondaryYield"]), use_secondary_yields, rootCrop)

            if vals.get("Year", 0) >= start_recording_out and not skip(vals.get("Harvest"), last_year):
                if SOC_STUDY:
                    out.append([
                        row_col,
                        rotation,
                        vals.get("Crop", "NA").replace("/", "_").replace(" ", "-"),
                        #prod_level,
                        vals.get("Year", "NA"),
                        #cyclelength(vals.get("Sowing"), vals.get("Harvest")),
                        #deltaSOC_perc(vals.get("SOCini", 0), vals.get("SOCend", 0)),
                        #vals.get("Rh", "NA"),
                        #vals.get("NEP", "NA"),
                        vals.get("Yield", "NA"),
                        #vals.get("AbBiom", "NA"),
                        vals.get("LAI", "NA"),
                        #vals.get("Stage", "NA"),
                        #vals.get("RelDev", "NA"),
                        #vals.get("Act_ET", "NA"),
                        #vals.get("Act_Ev", "NA"),
                        #vals.get("PercolationRate", "NA"),
                        #vals.get("Irrig", "NA"),
                        #vals.get("NLeach", "NA"),
                        #vals.get("ActNup", "NA"),
                        #vals.get("NFert", "NA"),
                        #vals.get("NOrgFert", "NA"),
                        #vals.get("N2O", "NA"),
                        #vals.get("Nstress", "NA"),
                        #vals.get("TraDef", "NA"),
                        export_residues,
                        return_residues,
                        vals.get("humusBalanceCarryOver", "NA")
                    ])
                
                else:
                    out.append([
                        row_col,
                        rotation,
                        vals.get("Crop", "NA").replace("/", "_").replace(" ", "-"),
                        prod_level,
                        vals.get("Year", "NA"),
                        cyclelength(vals.get("Sowing"), vals.get("Harvest")),
                        deltaSOC_perc(vals.get("SOCini", 0), vals.get("SOCend", 0)),
                        vals.get("Rh", "NA"),
                        vals.get("NEP", "NA"),
                        vals.get("Yield", "NA"),
                        vals.get("AbBiom", "NA"),
                        vals.get("LAI", "NA"),
                        vals.get("Stage", "NA"),
                        vals.get("RelDev", "NA"),
                        vals.get("Act_ET", "NA"),
                        vals.get("Act_Ev", "NA"),
                        vals.get("PercolationRate", "NA"),
                        #vals.get("Irrig", "NA"),
                        vals.get("NLeach", "NA"),
                        vals.get("ActNup", "NA"),
                        vals.get("NFert", "NA"),
                        vals.get("NOrgFert", "NA"),
                        vals.get("N2O", "NA"),
                        vals.get("Nstress", "NA"),
                        vals.get("TraDef", "NA"),
                        export_residues,
                        return_residues,
                        vals.get("humusBalanceCarryOver", "NA")
                        #main_cp_iteration
                    ])

    return out

def update_pheno_output(oids, row, col, rotation, prod_level, values, pheno_data, region_id):
    "create phenological related output lines"
    row_col = "{}{:03d}".format(row, col)
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                if oid["displayName"] != "":
                    oid_name = oid["displayName"]
                else:
                    oid_name = oid["name"]
                #oid_name = oid["displayName"] if oid["displayName"] != "" else oid_name = oid["name"]
                if isinstance(val, types.ListType):
                    for val_ in val:
                        vals[oid_name] = val_
                else:
                    vals[oid_name] = val
            pheno_data[region_id][vals.get("Crop")][vals.get("Year")].update(vals)

def write_data(region_id, year_data, crop_data, pheno_data, soc_data, suffix):
    "write data"

    basepath = "out/"
    if SAVE_TO_EXTERNAL_DRIVE:
        basepath = ext_drive_path

    path_to_crop_file = basepath + str(region_id) + suffix + "crop.csv"
    path_to_year_file = basepath + str(region_id) + suffix + "year.csv"
    path_to_pheno_file = basepath + str(region_id) + suffix + "pheno.csv"
    path_to_SOC_file = basepath + str(region_id) + suffix + "soc.csv"

    if not SOC_STUDY:
        if not os.path.isfile(path_to_year_file):
            with open(path_to_year_file, "w") as _:
                _.write("IDcell,rotation,prodlevel,year,deltaOC,SOCavg,CO2emission,NEP,ET,EV,waterperc,Nleach,Nup,Nminfert,Norgfert,N2Oem,Precip,yearTavg,KA5class,soiltype\n")

        with open(path_to_year_file, 'ab') as _:
            writer = csv.writer(_, delimiter=",")
            for row_ in year_data[region_id]:
                writer.writerow(row_)
            year_data[region_id] = []

    if not os.path.isfile(path_to_crop_file):
        with open(path_to_crop_file, "w") as _:
            if SOC_STUDY:
                _.write("IDcell,rotation,crop,year,yield,LAImax,ExportResidues,ReturnResidues,CarryOver\n")
            else:
                _.write("IDcell,rotation,crop,prodlevel,year,cyclelength,deltaOC,CO2emission,NEP,yield,agb,LAImax,Stageharv,RelDev,ET,EV,waterperc,Nleach,Nup,Nminfert,Norgfert,N2Oem,Nstress,Wstress,ExportResidues,ReturnResidues,CarryOver\n")

    with open(path_to_crop_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in crop_data[region_id]:
            writer.writerow(row_)
        crop_data[region_id] = []
    
    if SOC_STUDY:
        if not os.path.isfile(path_to_SOC_file):
            with open(path_to_SOC_file, "w") as _:
                _.write("row_col,rotation,p_id,KA5_txt,soil_type,orgN_kreis,unique_id,SOC7174,SOC7578,SOC7982,SOC8386,SOC8790,SOC9194,SOC9598,SOC9902,SOC0306,SOC0710,SOC1114,SOC1518,SOC1922,SOC2326,SOC2730,SOC3134,SOC3538,SOC3942,SOC4346,SOC4750\n")

        with open(path_to_SOC_file, 'ab') as _:
            writer = csv.writer(_, delimiter=",")
            for row_ in soc_data[region_id]:
                writer.writerow(row_)
            soc_data[region_id] = []
    
    #if not os.path.isfile(path_to_pheno_file):
    #    with open(path_to_pheno_file, "w") as _:
    #        _.write("crop,year,anthesis,maturity,harvest\n")

    #with open(path_to_pheno_file, 'ab') as _:
    #    writer = csv.writer(_, delimiter=",")
    #    for crop in pheno_data[region_id]:
    #        for year in pheno_data[region_id][crop]:
    #           row = [    
    #                crop,
    #                year,
    #                pheno_data[region_id][crop][year].get("anthesis", "NA"),
    #                pheno_data[region_id][crop][year].get("maturity", "NA"),
    #                pheno_data[region_id][crop][year].get("harvest", "NA")
    #            ]
    #            writer.writerow(row)
    #    pheno_data.clear()
    pheno_data.clear() 


def collector():
    "collect data from workers"

    year_data = defaultdict(list)
    crop_data = defaultdict(list)
    soc_data = defaultdict(list)
    pheno_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    #socket = context.socket(zmq.DEALER)
    #context.setsockopt(zmq.IDENTITY, "ts_sustag_nrw")
    if LOCAL_RUN:
        socket.connect("tcp://localhost:7777")
        #socket.connect("tcp://10.10.26.34:7777")
    else:
        socket.connect("tcp://cluster1:7777")
    socket.RCVTIMEO = 1000
    leave = False
    write_normal_output_files = False
    start_writing_lines_threshold = 1000
    while not leave:

        try:
            result = socket.recv_json()
        except:
            for region_id in crop_data.keys():
                if len(crop_data[region_id]) > 0:
                    write_data(region_id, year_data, crop_data, pheno_data, soc_data, suffix)
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", ""), " len(year_data): ", len((year_data.values()[:1] or [[]])[0])

            def True_False_string(str_in):
                out = True
                if str_in.lower() == "false":
                    out = False
                return out

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            rotation = ci_parts[0]
            prod_level = ci_parts[1]
            row_, col_ = ci_parts[3][1:-1].split("/")
            row, col = (int(row_), int(col_))
            region_id = ci_parts[4]
            main_cp_iteration = ci_parts[5]
            use_secondary_yields = True_False_string(ci_parts[6])
            start_recording_out = int(ci_parts[7])
            if SOC_STUDY:
                start_recording_out = 2045 #for SOC study, crop out is used only to check setup correctness
            residue_humus_balance = True_False_string(ci_parts[8])
            suffix = ci_parts[9]
            KA5_txt = ci_parts[10]
            soil_type = ci_parts[11]
            p_id = ci_parts[12]
            orgNkreis = ci_parts[13]
            unique_id = ci_parts[14]

            soc_res = [[]]
            row_col = "{}{:03d}".format(row, col)
            soc_res[0].append(str(row_col))
            soc_res[0].append(str(rotation))
            soc_res[0].append(str((p_id)))
            soc_res[0].append(str((KA5_txt)))
            soc_res[0].append(str((soil_type)))
            soc_res[0].append(str((orgNkreis)))
            soc_res[0].append(str((unique_id)))
            
            for data in result.get("data", []):
                results = data.get("results", [])
                orig_spec = data.get("origSpec", "")
                output_ids = data.get("outputIds", [])
                if len(results) > 0:
                    if orig_spec == '"yearly"':
                        if SOC_STUDY:
                            continue
                        else:
                            res = create_year_output(output_ids, row, col, rotation, prod_level, results, start_recording_out, KA5_txt, soil_type, main_cp_iteration)
                            year_data[region_id].extend(res)
                    elif orig_spec == '"crop"':
                        res = create_crop_output(output_ids, row, col, rotation, prod_level, results, use_secondary_yields, start_recording_out, residue_humus_balance, main_cp_iteration)
                        crop_data[region_id].extend(res)
                    elif re.search('from', orig_spec):
                        #only SOC is recorded with "from" "to"
                        soc_res[0].append(results[0][0] * 100)
                    #if re.search('anthesis', orig_spec) or re.search('maturity', orig_spec) or re.search('Harvest', orig_spec):
                    #    update_pheno_output(output_ids, row, col, rotation, prod_level, results, pheno_data, region_id)
            soc_data[region_id].extend(soc_res)

            for region_id in crop_data.keys():
                if len(crop_data[region_id]) > start_writing_lines_threshold:
                    write_data(region_id, year_data, crop_data, pheno_data, soc_data, suffix)

            i = i + 1

        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            with open("out/out-" + str(i) + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")

                for data in result.get("data", []):
                    results = data.get("results", [])
                    orig_spec = data.get("origSpec", "")
                    output_ids = data.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            i = i + 1


collector()

