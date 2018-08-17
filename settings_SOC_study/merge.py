import os
from os import listdir
from os.path import isfile, join
import csv

basepath = os.path.dirname(os.path.abspath(__file__))
filepath = basepath + "/merge/"
files = [f for f in listdir(filepath) if isfile(join(filepath, f))]

to_write = []
skip_header = False
for f in files:
    with open(filepath + f) as _:
        reader = csv.reader(_)
        if skip_header:
            reader.next()
        for line in reader:
            to_write.append(line)
        skip_header = True

with open(basepath + "/SOCini_unique_combinations.csv", "wb") as _:
    writer = csv.writer(_)
    for line in to_write:
        writer.writerow(line)

print "finished"