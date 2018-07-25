import os

basepath = os.path.dirname(os.path.abspath(__file__))

with open(basepath + "/MONICA_morris.py") as fp:
    for i, line in enumerate(fp):
        if "\xe2" in line:
            print i, repr(line)
