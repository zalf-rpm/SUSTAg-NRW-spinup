from subprocess import Popen

processes = 50
step = 4868 // processes
#print(step)

for i in range(0, processes+1):
    #print("start-row="+str(i*step+1), 
    #      "end-row="+str((i+1)*step if (i+1)*step < 4868 else 4868))
    Popen(["python", \
            "sampler_MONICA.py", \
            "number="+str(i), \
            "start-row="+str(i*step+1), \
            "end-row="+str((i+1)*step if (i+1)*step < 4868 else 4868)])