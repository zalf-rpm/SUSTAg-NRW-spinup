import os
from os import listdir
from os.path import isfile, join
import pandas as pd
import csv

out_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-11-18/"

extract_vars_cp = ["IDcell", "crop", "year", "rotation", "yield", "Nminfert", "Norgfert", "ExportResidues", "ReturnResidues", "CarryOver", "id", "bkr", "fert", "res", "cc"]
extract_vars_yr = ["IDcell", "year", "rotation", "Nleach", "SOCavg", "CO2emission", "N2Oem", "soiltype", "id", "bkr", "fert", "res", "cc"]
#target_id = "id0"

def merge(out_dir):
    'merge missing data to older results'
    missing_data_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-11-18/missing_data/"
    incomplete_data_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-11-18/incomplete_data/"
    

    missing_data_files = [f for f in listdir(missing_data_dir) if isfile(join(missing_data_dir, f))]
    for fname in missing_data_files:
        #if target_id not in fname:
        #    continue

        #load missing data from archive
        print("reading missing data" + fname)
        missing_df = pd.read_csv(missing_data_dir + "/" + fname)
        id_missing_cells = set(missing_df["IDcell"])

        #read incomplete data
        print("reading incomplete data" + fname)
        incomplete_df = pd.read_csv(incomplete_data_dir + "/" + fname)
        #drop cells with incomplete data
        to_drop = list(id_missing_cells)
        incomplete_df = incomplete_df[~incomplete_df['IDcell'].isin(to_drop)] # ~ works as negation

        #merge cleaned incomplete df and missing data
        print("merging and writing new file")
        merged_df = pd.concat([incomplete_df, missing_df])

        merged_df.to_csv(out_dir + fname, index=False)

def clean(out_dir):
    'remove cells (128) that do not have a kreis id'
    out_files = [f for f in listdir(out_dir) if isfile(join(out_dir, f))]
    drop_cells =[]
    with open("remove_out_cells.csv") as _:
        reader = csv.reader(_)
        for l in reader:
            drop_cells.append(l[0])

    for fname in out_files:
        #if target_id not in fname:
        #    continue
        print("cleaning data" + fname)
        my_df = pd.read_csv(out_dir + "/" + fname)
        my_df = my_df[~my_df['IDcell'].isin(drop_cells)] # ~ works as negation

        print("overwriting " + fname)
        my_df.to_csv(out_dir + fname, index=False)


def add_tags_light(dir_name):
    
    for filename in os.listdir(dir_name):
        out_files = [f for f in os.listdir(dir_name + "/tagged/") if os.path.isfile(os.path.join(dir_name + "/tagged/", f))]
        
        if not os.path.isfile(dir_name + filename):
            continue
        if filename in out_files:
            print("skipping " + filename)
            continue

        print("add tags - opening " + filename)
        
        with open(dir_name + filename) as file_:
            name_parts = filename.split(".")[0].split("_")
            bkr = name_parts[0]
            sim_id =name_parts[1][2:]
            tf = name_parts[2]
            fert = name_parts[3].split("-")[1]
            res = name_parts[4].split("-")[1]
            cc =  name_parts[5].split("-")[1]
            pl =  name_parts[6].split("-")[1]
            reader = csv.reader(file_, delimiter=",")

            with open(dir_name + "/tagged/" + filename, 'wb') as _:
                writer = csv.writer(_, delimiter=",")
            
                header = reader.next()
                header.append("bkr")
                header.append("id")
                header.append("tf")
                header.append("fert")
                header.append("res")
                header.append("cc")
                header.append("pl")
                writer.writerow(header)
                
                for row in reader:
                    row.append(bkr)
                    row.append(sim_id)
                    row.append(tf)
                    row.append(fert)
                    row.append(res)
                    row.append(cc)
                    row.append(pl)

                    writer.writerow(row)

def split_ioanna_light(dir_name, suffix, extract_vars, calc_res_ratio=False):
    dir_name += "tagged/"
    for filename in os.listdir(dir_name):
        #if filename != "129_id0_continuous_fert-base_res-base_cc-25_pl-WLNLrain_crop.csv":
        #    continue
        if ".csv" in filename and suffix in filename:
            print("split out files - opening " + filename)
            fname = filename.split("_")
            
            with open(dir_name + filename) as file_:
                reader = csv.reader(file_, delimiter=",")
                header = reader.next()
                field_map = {}
                for i in range(len(header)):
                    field_map[header[i]] = i
                #rowcount=0
                
                out_dir = dir_name + "splitted/" + fname[1][2:]
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                out_name = out_dir + "/" + filename

                with open(out_name, 'wb') as _:
                    writer = csv.writer(_, delimiter=",")
                    out_header = []
                    for ev in extract_vars:
                        out_header.append(ev)
                    if calc_res_ratio:
                        out_header.append("Exp_ratio")
                    writer.writerow(out_header)

                    for row in reader:
                        #rowcount +=1
                        line = []
                        for v in extract_vars:
                            line.append(row[field_map[v]])
                        if calc_res_ratio:
                            ex_res = float(row[field_map["ExportResidues"]])
                            ret_res = float(row[field_map["ReturnResidues"]])
                            try:
                                ratio = round(ex_res / (ex_res + ret_res), 2)
                            except:
                                ratio = 0
                            line.append(str(ratio))
                        writer.writerow(line)

            print(filename + " done!")

def aggregate_results_paper1(base_dir):

    time_slices = [
        {
            "start": 1985,
            "end": 2004,
            "relative": [],
            "is_reference": True
        },
        {
            "start": 2021,
            "end": 2040,
            "relative": [],
            "is_reference": True
        },
        {
            "start": 2041,
            "end": 2060,
            "relative": [],
            "is_reference": True
        }
    ]
    time_slices_SOC = [
        {
            "start": 2000,
            "end": 2004,
            "relative": [],
            "is_reference": True
        },
        {
            "start": 2026,
            "end": 2030,
            "relative": ["SOCavg"],
            "is_reference": False
        },
        {
            "start": 2046,
            "end": 2050,
            "relative": ["SOCavg"],
            "is_reference": False
        }
    ]

    no_avg = ["IDcell", "crop", "rotation", "soiltype", "id", "bkr", "tf", "fert", "res", "cc", "pl"]

    def one_liner(df, calcSOC=True):
        line = []
        ref_vars = {}
        #add plain output
        for column in df:
            if column == "year":
                continue
            if column in no_avg:
                line.append(df[column].iloc[0])
        #add sliced output
        for column in df:
            if column == "year":
                continue
            if column not in no_avg:
                if column == "SOCavg":
                    continue
                for ts in time_slices:
                    var_arr = df[column].loc[(df["year"] >= ts["start"]) & 
                                                (df["year"] <= ts["end"])]
                    my_var = round(var_arr.mean(), 2)
                    line.append(my_var)
        #SOC has a different slicing
        if calcSOC:
            column = "SOCavg"
            for ts in time_slices_SOC:
                var_arr = df[column].loc[(df["year"] >= ts["start"]) & 
                                            (df["year"] <= ts["end"])]
                my_var = round(var_arr.mean() * 100, 2) #kg kg-1 -> %
                #add to reference map
                if ts["is_reference"]:
                    ref_vars[column] = my_var
                line.append(my_var) #abs val
                #calc relative change if needed
                if column in ts["relative"]:
                    my_var = round((my_var - ref_vars[column]) / ref_vars[column] * 100, 2)
                    line.append(my_var) #rel val
        return line
    
    def prepare_header(df, SOC_columns=False):
        header = []
        for column in df:
            if column == "year":
                continue
            if column not in no_avg:
                #other vars will be "sliced", see below
                continue
            header.append(column)
        #add sliced output
        for column in df:
            if column == "year":
                continue
            if column not in no_avg:
                if column == "SOCavg":
                    continue
                for ts in time_slices:
                    var_name = column
                    var_name += "_" + str(ts["start"])
                    var_name += "_" + str(ts["end"])
                    header.append(var_name) #abs val
                    if column in ts["relative"]:
                        var_name += "_rel" 
                        header.append(var_name) #rel val
        #SOC has a different slicing
        column = "SOCavg"
        if SOC_columns:
            for ts in time_slices_SOC:
                var_name = column
                var_name += "_" + str(ts["start"])
                var_name += "_" + str(ts["end"])
                header.append(var_name) #abs val
                if column in ts["relative"]:
                    var_name += "_rel"
                    header.append(var_name) #rel val
        return header
    
    base_dir += "tagged/splitted/"
    
    for dirName, subdirList, fileList in os.walk(base_dir):
        print('Found directory: %s' % dirName)
        if "aggregated" in dirName:
            #no need to process processed files :)
            continue            
        for fname in fileList:
            #if "134_id0" not in fname:
            #    continue
            #if "crop" not in fname:
            #    continue
            print("aggregation - reading " + fname)
            towrite = []
            header = []
            #print('\t%s' % fname)
            my_df = pd.read_csv(dirName + "/" + fname)
            #print my_df.head()
            id_cells = set(my_df["IDcell"])

            for cell in id_cells:
                #if cell != 434195:
                #    continue
                cell_data = my_df.loc[(my_df["IDcell"] == cell)]
                rotations = set(cell_data["rotation"])

                for rot in rotations:
                    #if rot != 7130:
                    #    continue
                    cell_rot_data = cell_data.loc[(cell_data["rotation"] == rot)]

                    if "_year" in fname:
                        if len(header) == 0:
                            header = prepare_header(cell_rot_data, SOC_columns=True)
                            towrite.append(header)
                        towrite.append(one_liner(cell_rot_data))

                    if "_crop" in fname:
                        if len(header) == 0:
                            header = prepare_header(cell_rot_data)
                            towrite.append(header)
                        crops = set(cell_rot_data["crop"])
                        for cp in crops:
                            #if cp != "mustard_":
                            #    continue
                            cell_rot_crop_data = cell_rot_data.loc[(cell_rot_data["crop"] == cp)]
                            towrite.append(one_liner(cell_rot_crop_data, calcSOC=False))
            
            out_dir = dirName + "/aggregated/"
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            with open(out_dir + "/" + fname, "wb") as _:
                writer = csv.writer(_)
                for line in towrite:
                    writer.writerow(line)        

def concatenate_files(base_dir):
    final_dir = base_dir + "final/"   
    base_dir += "tagged/splitted/"
    
    id_sim = ""
    for dirName, subdirList, fileList in os.walk(base_dir):
        print('Concatenate - Found directory: %s' % dirName)
        if "aggregated" in dirName:
            for f_type in ["_year", "_crop"]:
                dframes = []
                for fname in fileList:
                    fn = fname.split("_")
                    id_sim = fn[1][2:]
                    if f_type in fname:
                        print(" appending " + fname)
                        dframes.append(pd.read_csv(dirName + "/" + fname))
                print("concatenating data frames...")
                merged_df = pd.concat(dframes)
                merged_df.to_csv(final_dir + id_sim + f_type + ".csv")
                print("id sim " + id_sim + f_type + " processed!")

#merge(out_dir)
#clean(out_dir)
#add_tags_light(out_dir)
#split_ioanna_light(out_dir, "_crop", extract_vars_cp, calc_res_ratio=True)
#split_ioanna_light(out_dir, "_year", extract_vars_yr)
#aggregate_results_paper1(out_dir)
concatenate_files(out_dir)