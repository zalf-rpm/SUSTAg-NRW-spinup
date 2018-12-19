#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Tommaso Stella <tommaso.stella@zalf.de>
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import json
from collections import defaultdict
from os import listdir
from os.path import isfile, join
import pandas as pd

def report_expected_sims():

    rotations_file = "json_templates/rotations_dynamic_harv_Nbaseline.json"
        
    def load_rotations(rotations_file, spinup):
        with open(rotations_file) as _:
            rotations = json.load(_)
            #identify rotations with codes
            rots_info = {}
            for bkr, rots in rotations.iteritems():
                for rot in rots.iteritems():
                    rot_code = int(rot[0])
                    my_rot = []
                    for cm in rot[1]:#["worksteps"]:
                        for ws in range(len(cm["worksteps"])):
                            if cm["worksteps"][ws]["type"] == "Sowing":
                                my_rot.append(cm["worksteps"][ws]["crop"][2])
                    #for each crop, identify previous main one (needed to determine expected N availability)
                    rot_info = []
                    for i in range(len(my_rot)):
                        cm_info = {}
                        current_cp = my_rot[i]
                        if i != 0:
                            previous_cp = my_rot[i-1]
                        else:
                            previous_cp = my_rot[-1]
                        cm_info["current"] = current_cp
                        cm_info["previous"] = previous_cp
                        rot_info.append(cm_info)
                    rots_info[rot_code] = rot_info
            return rotations, rots_info
    
    rotations, rots_info = load_rotations(rotations_file, spinup=False)
        
    def read_general_metadata(path_to_file):
        "read metadata file"
        with open(path_to_file) as file_:
            data = {}
            reader = csv.reader(file_, delimiter="\t")
            reader.next()
            for row in reader:
                if int(row[1]) != 1:
                    continue
                data[(int(row[2]), int(row[3]))] = {
                    "subpath-climate.csv": row[9],
                    "latitude": float(row[13]),
                    "elevation": float(row[14])
                }
            return data

    general_metadata = read_general_metadata("NRW_General_Metadata.csv")

    def read_ascii_grid(path_to_file, include_no_data=False, row_offset=0, col_offset=0):
        "read an ascii grid into a map, without the no-data values"
        def int_or_float(s):
            try:
                return int(s)
            except ValueError:
                return float(s)

        with open(path_to_file) as file_:
            data = {}
            #skip the header (first 6 lines)
            for _ in range(0, 6):
                file_.next()
            row = -1
            for line in file_:
                row += 1
                col = -1
                for col_str in line.strip().split(" "):
                    col += 1
                    if not include_no_data and int_or_float(col_str) == -9999:
                        continue
                    data[(row_offset+row, col_offset+col)] = int_or_float(col_str)

            return data

    #offset is used to match info in general metadata and soil database
    soil_ids = read_ascii_grid("asc_grids/soil-profile-id_nrw_gk3.asc", row_offset=282)
    bkr_ids = read_ascii_grid("asc_grids/bkr_nrw_gk3.asc", row_offset=282)
    lu_ids = read_ascii_grid("asc_grids/lu_resampled.asc", row_offset=282)
    kreise_ids = read_ascii_grid("asc_grids/kreise_matrix.asc", row_offset=282)

    no_kreis = 0
    simulated_cells = 0
    remove_out_cells = set()

    with open("report_expected_sims.csv", "wb") as _:
        writer = csv.writer(_)
        writer.writerow(["bkr", "cellID", "rotation", "permutations"])

        for (row, col), gmd in general_metadata.iteritems():

            row_col = "{}{:03d}".format(row, col)

            if (row, col) in soil_ids and (row, col) in bkr_ids and (row, col) in lu_ids:

                bkr_id = bkr_ids[(row, col)]

                if (row, col) in kreise_ids:
                    kreis_id = kreise_ids[(row, col)]
                    #bkr2lk[bkr_id].add(kreis_id)
                else:
                    no_kreis += 1
                    remove_out_cells.add(row_col)
                    print "-----------------------------------------------------------"
                    print "kreis not found for calculation of organic N, skipping cell"
                    print "-----------------------------------------------------------"
                    continue

                simulated_cells += 1

                for rot_id, rotation in rotations[str(bkr_id)].iteritems():
                    permutations = len(rots_info[int(rot_id)])
                    writer.writerow([bkr_id, row_col, rot_id, permutations])
    
    with open("remove_out_cells.csv", "wb") as _:
        writer = csv.writer(_)
        for rc in remove_out_cells:
            writer.writerow([rc])

    print("finished! Total cells: " + str(simulated_cells) + " No kreis: " + str(no_kreis))

#report_expected_sims()

def check_cluster_output():
    #load info expected out
    bkr_cell_rot_perms = defaultdict(lambda: defaultdict(dict))
    with open("report_expected_sims.csv") as _:
        reader = csv.reader(_)
        reader.next()
        for line in reader:
            bkr = line[0]
            cell = line[1]
            rot = line[2]
            perms = line[3]
            bkr_cell_rot_perms[bkr][cell][rot] = perms
    
    #initialize report file
    report_file = "report_missing_sims_id22.csv"
    with open(report_file, "wb") as _:
        writer = csv.writer(_)
        writer.writerow(["sim_id","bkr","cell","rot","expected perms","actual perms"])
    
    #load out from archive
    base_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-11-18/"
    out_files = [f for f in listdir(base_dir) if isfile(join(base_dir, f))]
    for fname in out_files:
        if "_crop" in fname:
            #skip, use only year files
            continue
        if "id22" not in fname:
            continue
        print("reading " + fname)
        sim_id = fname.split("_")[1]
        bkr = fname.split("_")[0]
        my_df = pd.read_csv(base_dir + "/" + fname)
        my_df = my_df.loc[(my_df["year"] == 1971)] #no need to work with the entire set of years
        id_cells_expected = set(bkr_cell_rot_perms[bkr].keys())
        id_cells_simulated = set(my_df["IDcell"])
        
        with open(report_file, "ab") as _:
            #check 1: are all the cells there?
            writer = csv.writer(_)
            for expected_cell in id_cells_expected:
                if int(expected_cell) not in id_cells_simulated:
                    print("Missing cell: " + str(expected_cell))
                    for rot in bkr_cell_rot_perms[bkr][expected_cell].keys():
                        expected_perms = bkr_cell_rot_perms[bkr][expected_cell][rot]
                        writer.writerow([sim_id, bkr, expected_cell, rot, expected_perms, 0])
            
            #check 2: missing rotations?
            for sim_cell in id_cells_simulated:
                sim_rotations = set(my_df.loc[(my_df["IDcell"] == sim_cell)]["rotation"])
                for expected_rot in bkr_cell_rot_perms[bkr][str(sim_cell)].keys():
                    expected_perms = bkr_cell_rot_perms[bkr][str(sim_cell)][expected_rot]
                    if int(expected_rot) not in sim_rotations:
                        print("Cell: " + str(sim_cell) + " Missing rot: " + str(expected_rot))
                        writer.writerow([sim_id, bkr, sim_cell, expected_rot, expected_perms, 0])
                    else:
                        #check 3: missing permutations?
                        sim_perms = my_df.loc[(my_df["IDcell"] == sim_cell) &
                                            (my_df["rotation"] == int(expected_rot))].shape[0]         
                        if sim_perms != int(expected_perms):
                            print("Cell: " + str(sim_cell) + " Missing permutations for rot: " + str(expected_rot))
                            writer.writerow([sim_id, bkr, sim_cell, expected_rot, expected_perms, sim_perms])
    print("finished!")

check_cluster_output()


