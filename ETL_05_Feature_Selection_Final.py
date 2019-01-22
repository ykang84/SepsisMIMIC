import pandas as pd
import numpy as np
import os
import datetime
from datetime import timedelta

ac = pd.read_csv("../data/antibiotics_culture.csv")
icu = pd.read_csv("../data/ICUSTAYS.csv")

# First need to get the ICU admit time for calculating hours

icu = icu[icu["DBSOURCE"] == "metavision"]
icu_unique = pd.DataFrame(icu[["SUBJECT_ID", "HADM_ID", "ICUSTAY_ID"]].groupby(["SUBJECT_ID", "HADM_ID"])["ICUSTAY_ID"].agg(len))

icu_unique["SUBJECT_ID"] = [a for a, b in list(icu_unique.index)]
icu_unique["HADM_ID"] = [b for a, b in list(icu_unique.index)]
icu_unique["count"] = icu_unique["ICUSTAY_ID"]
icu_unique = icu_unique.reset_index(drop = True)
icu_unique = icu_unique[["SUBJECT_ID", "HADM_ID", "count"]][icu_unique["count"] == 1]
icu_unique = icu_unique.merge(icu[["SUBJECT_ID", "HADM_ID", "INTIME"]], left_on = ["SUBJECT_ID", "HADM_ID"], right_on = ["SUBJECT_ID", "HADM_ID"], how =  "left")
ac = ac.merge(icu_unique[["SUBJECT_ID", "HADM_ID", "INTIME"]], left_on = ["SUBJECT_ID", "HADM_ID"], right_on = ["SUBJECT_ID", "HADM_ID"], how =  "outer")

print(len(ac.index))
print(len(ac[ac["antibiotics_prescribed"] == 0])) 

# Now the antibiotics culture file also contains the icu admit time.
# Next, we need to merge the antibiotics culture table with the ETL_DATA
# We will read the data in as chunks since it is a relative large file.

c_size = 50000
output_file = "ETL_04_output.csv"

if os.path.isfile(output_file):
    os.remove(output_file)

print(ac.columns)
datetimeFormat = "%Y-%m-%d %H:%M:%S"#.%f"
total_chunks = 0
sepsis_martin = pd.read_csv("../data/sepsis_labeled_martin.csv")

for chunk in pd.read_csv("../data/ETL_output.csv", chunksize = c_size, low_memory = False):
    total_chunks += len(chunk)
    chunk = chunk.merge(ac, left_on = ["SUBJECT_ID", "HADM_ID"], right_on = ["SUBJECT_ID", "HADM_ID"], how =  "left")
    chunk["culture_hours"] = [int((datetime.datetime.strptime(a, datetimeFormat) - datetime.datetime.strptime(b, datetimeFormat)).total_seconds() / 3600) if isinstance(a, str) and isinstance(b, str) else np.nan for a, b in zip(list(chunk["culture_time"]), list(chunk["INTIME"]))]
    del chunk["culture_time"]
    chunk["event_hours"] = [int((datetime.datetime.strptime(a, datetimeFormat) - datetime.datetime.strptime(b, datetimeFormat)).total_seconds() / 3600) if isinstance(a, str) and isinstance(b, str) else np.nan for a, b in zip(list(chunk["CHARTTIME"]), list(chunk["INTIME"]))]
    del chunk["CHARTTIME"]
    del chunk["INTIME"]
    chunk = chunk.merge(sepsis_martin, left_on = ["SUBJECT_ID", "HADM_ID"], right_on = ["subject_id", "hadm_id"], how =  "left")
    del chunk["subject_id"]
    del chunk["hadm_id"]

    chunk["positive_culture"] = chunk["positive_culture"].fillna(0)
    chunk["negative_culture"] = chunk["negative_culture"].fillna(0)
    chunk["antibiotics_prescribed"] = chunk["antibiotics_prescribed"].fillna(0)
    chunk["culture_hours"] = chunk["culture_hours"].fillna(-99999)
    chunk = chunk[(chunk["culture_hours"] == -99999) | (chunk["culture_hours"] >= 7)]
    chunk = chunk[(chunk["event_hours"] <= chunk["culture_hours"]) | (chunk["culture_hours"] == -99999)]
    #chunk = chunk.dropna()

    # If file does not exist, write header 
    if not os.path.isfile(output_file):
        chunk.to_csv(output_file, header = True, index = False)
    else: # else it exists so append without writing the header
        chunk.to_csv(output_file, mode='a', header = False, index = False)
    
    print("Appending " + str(len(chunk)) + "...")    
    print(str(total_chunks))

