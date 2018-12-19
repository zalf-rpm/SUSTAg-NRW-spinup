from os import listdir
from os.path import isfile, join
import pandas as pd
import csv

out_dir = "out/"#"Z:/projects/sustag/spinup-version/out_paper1/22-10-18/"
target_id = "id0"

def merge():
    'merge missing data to older results'
    missing_data_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-10-18/missing_data/"
    incomplete_data_dir = "Z:/projects/sustag/spinup-version/out_paper1/22-10-18/incomplete_data/"
    

    missing_data_files = [f for f in listdir(missing_data_dir) if isfile(join(missing_data_dir, f))]
    for fname in missing_data_files:
        if target_id not in fname:
            continue

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

def clean():
    'remove cells (128) that do not have a kreis id'
    out_files = [f for f in listdir(out_dir) if isfile(join(out_dir, f))]
    drop_cells =[]
    with open("remove_out_cells.csv") as _:
        reader = csv.reader(_)
        for l in reader:
            drop_cells.append(l[0])

    for fname in out_files:
        if target_id not in fname:
            continue
        print("cleaning data" + fname)
        my_df = pd.read_csv(out_dir + "/" + fname)
        my_df = my_df[~my_df['IDcell'].isin(drop_cells)] # ~ works as negation

        print("overwriting " + fname)
        my_df.to_csv(out_dir + fname, index=False)

merge()
clean()