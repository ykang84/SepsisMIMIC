import pandas as pd
import os
import numpy as np

c_size = 100000
output_file = "../data/routine_vitals_events.csv"

# Read in the list of routine vitals:

r_vitals = pd.read_csv("../data/routine_vital_signs.csv")
r_vitals = list(r_vitals["ITEMID"])

if os.path.isfile(output_file):
    os.remove(output_file)

# Read in the very large CHARTEVENTS.csv file in chunks. Filter 
# the file to include only routine vitals. Save the file
# to the output file directory/file name.

total_chunks = 0
for chunk in pd.read_csv("CHARTEVENTS.csv", chunksize = c_size, low_memory = False):
    total_chunks += len(chunk)
    chunk = chunk[chunk["ITEMID"].isin(r_vitals)]
    del chunk['ROW_ID']
    del chunk['RESULTSTATUS']
    del chunk['STOPPED']
    del chunk['STORETIME']
    # If file does not exist, write header 
    if not os.path.isfile(output_file):
        chunk.to_csv(output_file, header = True, index = False)
    else: # else it exists so append without writing the header
        chunk.to_csv(output_file, mode='a', header=False, index = False)
    
    print("Appending " + str(len(chunk)) + " routine events...")    
    print(str(round(total_chunks / 330712483 * 100, 3))  + "%")
