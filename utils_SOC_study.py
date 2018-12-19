import csv
import pandas as pd
import os
import json

def aggregate_SOC_results(exclude_files):
    print ("start crunching data - aggregate permutations of rotations...")

    def one_liner_SOC(df, exp_id, res_mgt, cc):
        refSOC = df["SOC9902"].mean()
        line = [exp_id, res_mgt, cc, refSOC]
        no_avg = ["row_col", "rotation", "p_id", "KA5_txt", "soil_type", "orgN_kreis", "unique_id", "refSOC9902"]
        for column in df:
            if column in no_avg:
                line.append(df[column].iloc[0])
            else:
                line.append(round((df[column].mean() - refSOC)/refSOC, 4))
        return line
    
    def prepare_header(df):
        header = ["exp_id", "res_mgt", "cc", "refSOC9902"]
        for column in df:
            header.append(column)
        return header

    basepath = os.path.dirname(os.path.abspath(__file__))
    file_dir = basepath + "/out_rerun/" #TODO change this if needed
    SOC_files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f))]
    processed_files = 0
    total_files = len(SOC_files)

    missing_data = []

    for fname in SOC_files:
        skip_file = False
        for exc in exclude_files:
            if exc in fname:
                skip_file = True
        if skip_file:
            processed_files +=1
            print("skipping " + fname)
            continue
        processed_files +=1
        print("processing " + fname + ": file " + str(processed_files) + " out of: " + str(total_files))
        split_name = fname.split("_")
        exp_id = split_name[1]
        res_mgt = split_name[4].replace("res-", "")
        cc = split_name[5].replace("cc-", "")
        towrite = []
        header = []
        #print('\t%s' % fname)
        my_df = pd.read_csv(file_dir + fname)
        #print my_df.head()
        id_cells = set(my_df["row_col"])

        for cell in id_cells:
            cell_data = my_df.loc[(my_df["row_col"] == cell)]
            rotations = set(cell_data["rotation"])
            p_ids = set(cell_data["p_id"])

            for rot in rotations:
                for p_id in p_ids:
                    cell_rot_p_id_data = cell_data.loc[(cell_data["rotation"] == rot) &
                                                  (cell_data["p_id"] == p_id)]

                    if len(header) == 0:
                        header = prepare_header(cell_rot_p_id_data)
                        towrite.append(header)
                    try:
                        towrite.append(one_liner_SOC(cell_rot_p_id_data, exp_id, res_mgt, cc))
                    except:
                        missing = (fname, cell, rot, p_id)
                        print(missing)
                        missing_data.append(missing)
                
        out_dir = file_dir + "/aggregated/"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(out_dir + "/" + fname.replace(".csv", "_crunch.csv"), "wb") as _:
            writer = csv.writer(_)
            for line in towrite:
                writer.writerow(line)
    print("missing:")
    print missing_data
    print("done!")

def concatenate_files():
    print ("Starting concatentation...")    
    basepath = os.path.dirname(os.path.abspath(__file__))
    file_dir = "Z:/projects/sustag/spinup-version/out_paper2/final_aggregated/" #basepath + "/SOC_out/aggregated/" TODO modify if necessary
    SOC_files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f))]
    dframes = []

    for fname in SOC_files:
        print("appending " + fname)
        dframes.append(pd.read_csv(file_dir + "/" + fname))
    
    out_dir = basepath + "/SOC_out/final/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    print("concatenating data frames...")
    merged_df = pd.concat(dframes)
    merged_df.to_csv(out_dir + "00_SOC_data.csv")
    print("done!")

def add_humbal_info_SOC():
    print("adding humbal information to the crunched output")
    basepath = os.path.dirname(os.path.abspath(__file__))
    file_dir = basepath + "/out_ids_12-23/aggregated/"
    SOC_files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f))]

    ids_2_humbal = {
        "id12": "humbal_0",
        "id14": "humbal_0",
        "id15": "humbal_0",

        "id16": "humbal_200",
        "id18": "humbal_200",
        "id19": "humbal_200",

        "id20": "humbal_400",
        "id22": "humbal_400",
        "id23": "humbal_400"
    }
    
    for fname in SOC_files:
        split_name = fname.split("_")
        exp_id = split_name[1]
        humbal = ids_2_humbal[exp_id]

        print("processing " + fname)
        towrite = []
        with open(file_dir + fname) as _:
            reader = csv.reader(_)
            towrite.append(reader.next())
            for l in reader:
                l[1] = humbal
                towrite.append(l)
        with open(file_dir + fname, "wb") as _:
            writer = csv.writer(_)
            for l in towrite:
                writer.writerow(l)

    print("done!")

exclude_files = ["_crop", "id1_", "id5_", "id9_"]
#aggregate_SOC_results(exclude_files)
#add_humbal_info_SOC()
concatenate_files()

def rotation_info():
    print("gathering rotation infos...")
    basepath = os.path.dirname(os.path.abspath(__file__))
    def retrieve_rot_info(rotations_file):
        COVER_BEFORE = ["SM", "GM", "SB", "PO", "SBee"]
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
                    cc_slots = 0                    
                    for i in range(len(my_rot)):
                        current_cp = my_rot[i]
                        if current_cp in COVER_BEFORE:
                            cc_slots += 1
                    rots_info[rot_code] = {}
                    rots_info[rot_code]["rotation"] = my_rot
                    rots_info[rot_code]["cc_slots"] = cc_slots
                    rots_info[rot_code]["cc_slots_rel"] = int(cc_slots / float(len(my_rot)) * 100)

        return rots_info

    rotations_file = "json_templates/rotations_dynamic_harv_Ndem.json"
    rots_info = retrieve_rot_info(rotations_file)

    with open(basepath + "/rot_cc-slots.csv", "wb") as _:
        writer = csv.writer(_)
        header = ["rot_code", "cc_slots", "cc_slots_rel", "crops"]
        writer.writerow(header)

        for rot in rots_info:
            line = []
            rot_str = ""
            line.append(rot)
            line.append(rots_info[rot]["cc_slots"])
            line.append(rots_info[rot]["cc_slots_rel"])
            for cp in rots_info[rot]["rotation"]:
                rot_str += str(cp) + "-"
            line.append(rot_str)
            writer.writerow(line)
    print("done!")

#rotation_info()

def add_cc_info_SOC():
    print("adding information to the crunched output")
    basepath = os.path.dirname(os.path.abspath(__file__))

    #retrieve info on cc presence in the rotations
    cc_slots_rel = {}
    with open(basepath + "/rot_cc-slots.csv") as _:
        reader = csv.reader(_)
        reader.next()
        for l in reader:
            rot_code = l[0]
            slot_freq = float(l[2])
            cc_slots_rel[rot_code] = slot_freq
    
    #calculate return time of cc and add it to out file
    with open (basepath + "/SOC_out/final/00_SOC_data_plus.csv", "wb") as outfile:
        writer = csv.writer(outfile)
        with open(basepath + "/SOC_out/final/00_SOC_data.csv") as _:
            reader = csv.reader(_)
            header = reader.next()
            header.append("cc_return_t")
            writer.writerow(header)
            for l in reader:
                rot_code = l[6]  
                cc_freq = float(l[3])
                slot_freq = cc_slots_rel[rot_code]
                cc_return_t = 0
                if cc_freq != 0 and slot_freq != 0:
                    cc_return_t = int(1/(cc_freq/100 * slot_freq/100))
                l.append(cc_return_t)
                writer.writerow(l)
    print("done!")

#add_cc_info_SOC()
