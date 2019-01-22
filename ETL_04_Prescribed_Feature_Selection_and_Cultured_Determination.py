import pandas as pd
import numpy as np
import os
import subprocess

drugs = pd.read_csv("../data/PRESCRIPTIONS.csv", low_memory = False)

print(len(drugs.index))

drugs = drugs[["SUBJECT_ID","HADM_ID","ICUSTAY_ID","DRUG","DRUG_NAME_POE","DRUG_NAME_GENERIC","ROUTE"]]
# Removing routes that do not indicate a blood infection:
drugs = drugs[drugs.ROUTE != 'OU']
drugs = drugs[drugs.ROUTE != 'OS']
drugs = drugs[drugs.ROUTE != 'OD']
drugs = drugs[drugs.ROUTE != 'AU']
drugs = drugs[drugs.ROUTE != 'AS']
drugs = drugs[drugs.ROUTE != 'AD']
drugs = drugs[drugs.ROUTE != 'TP']
drugs = drugs[drugs.ROUTE != 'LEFT EAR']
drugs = drugs[drugs.ROUTE != 'RIGHT EAR']
drugs = drugs[drugs.ROUTE != 'BOTH EARS']

print(len(drugs.index))
# Removing other drugs that do not indicate a blood infection:
#print(list(drugs["DRUG"].map(lambda d: True if sum([True if a in d.lower() else False for a in ["cream", "desensitization", "opthalmic oint", "gel"]]) == 0 else False)))
drugs = drugs.loc[list(drugs["DRUG"].map(lambda d: True if sum([True if a in str(d).lower() else False for a in ["cream", "desensitization", "opthalmic oint", "gel"]]) == 0 else False))]
drugs = drugs.loc[list(drugs["DRUG_NAME_GENERIC"].map(lambda d: True if sum([True if a in str(d).lower() else False for a in ["cream", "desensitization", "opthalmic oint", "gel"]]) == 0 else False))]
print(len(drugs.index))

antibiotics = ['adoxa', 'ala-tet', 'alodox', 'amikacin', 'amoxicillin', 'clavulanate', 'ampicillin', 'augmentin', 'avelox', 'avidoxy', 'azactam', 'azithromycin', 'aztreonam', 'axetil', 'bactocill', 'bactrim', 'bethkis', 'biaxin', 'bicillin l-a', 'cayston', 'cefazolin', 'cedax', 'cefoxitin', 'ceftazidime', 'cefaclor', 'cefadroxil', 'cefdinir', 'cefditoren', 'cefepime', 'cefotetan', 'cefotaxime', 'cefpodoxime', 'cefprozil', 'ceftibuten', 'ceftin', 'cefuroxime', 'cephalexin', 'chloramphenicol', 'cipro', 'claforan', 'clarithromycin', 'cleocin', 'clindamycin', 'cubicin', 'dicloxacillin', 'doryx', 'doxycycline', 'duricef', 'dynacin', 'ery-tab', 'eryped', 'eryc', 'erythrocin', 'erythromycin', 'factive', 'flagyl', 'fortaz', 'furadantin', 'garamycin', 'gentamicin', 'kanamycin', 'keflex', 'ketek', 'levaquin', 'levofloxacin', 'lincocin', 'macrobid', 'macrodantin', 'maxipime', 'mefoxin', 'metronidazole', 'minocin', 'minocycline', 'monodox', 'monurol', 'morgidox', 'moxatag', 'moxifloxacin', 'myrac', 'nafcillin sodium', 'nicazel doxy 30', 'nitrofurantoin', 'noroxin', 'ocudox', 'ofloxacin', 'omnicef', 'oracea', 'oraxyl', 'oxacillin', 'pc pen vk', 'pce dispertab', 'panixine', 'pediazole', 'penicillin', 'periostat', 'pfizerpen', 'piperacillin', 'tazobactam', 'primsol', 'proquin', 'raniclor', 'rifadin', 'rifampin', 'rocephin', 'smz-tmp', 'septra', 'solodyn', 'spectracef', 'streptomycin', 'sulfadiazine', 'sulfamethoxazole', 'trimethoprim', 'sulfatrim', 'sulfisoxazole', 'suprax', 'synercid', 'tazicef', 'tetracycline', 'timentin', 'tobi', 'tobramycin', 'timentin', 'tobi', 'tobramycin', 'unasyn', 'vancocin', 'vancomycin', 'vantin', 'vibativ', 'vibra-tabs', 'vibramycin', 'zinacef', 'zithromax', 'zmax', 'zosyn', 'zyvox']

DRUG = [str(a).lower() for a in list(drugs["DRUG"])]
DRUG_GENERIC = [str(a).lower() for a in list(drugs["DRUG_NAME_GENERIC"])]
DCHECK = [False] * len(DRUG)
DGCHECK = [False] * len(DRUG_GENERIC)

# Checking if drug is an antibiotic (using generic and ordinary drug names):
for a in antibiotics:
    for i in range(len(DCHECK)):
        DCHECK[i] = DCHECK[i] or a in DRUG[i]
    for i in range(len(DCHECK)):
        DGCHECK[i] = DGCHECK[i] or a in DRUG_GENERIC[i]

DCHECK = zip(DCHECK, DGCHECK)
drugs["antibiotics"] = [a[0] or a[1] for a in DCHECK]

drugs = drugs[drugs["antibiotics"]]
drugs[["SUBJECT_ID", "HADM_ID"]].drop_duplicates().dropna().to_csv("../data/antibiotics.csv", index = False)

antibiotics = pd.read_csv("../data/antibiotics.csv")
cultures = pd.read_csv("../data/MICROBIOLOGYEVENTS.csv")
cultures = cultures[cultures["SPEC_TYPE_DESC"] == "BLOOD CULTURE"]
cultures["positive_culture"] = [1 if not pd.isna(a) else 0 for a in cultures["ORG_NAME"]]
cultures["negative_culture"] = [0 if a else 1 for a in cultures["positive_culture"]]
cultures = cultures[["SUBJECT_ID", "HADM_ID", "CHARTTIME", "positive_culture", "negative_culture"]]
positive_group = pd.DataFrame(cultures.groupby(["SUBJECT_ID", "HADM_ID"])["positive_culture"].agg(max))
print(len(positive_group.index))
print(len(cultures.index))
cultures = positive_group.merge(cultures, left_on = ["SUBJECT_ID", "HADM_ID", "positive_culture"], right_on = ["SUBJECT_ID", "HADM_ID", "positive_culture"], how = "inner").dropna()
print(cultures)

time_group = pd.DataFrame(cultures.fillna("999999999999999999999").groupby(["SUBJECT_ID", "HADM_ID", "positive_culture", "negative_culture"])["CHARTTIME"].agg(min))

cultures = time_group.merge(cultures, left_on = ["SUBJECT_ID", "HADM_ID", "positive_culture", "negative_culture", "CHARTTIME"], right_on = ["SUBJECT_ID", "HADM_ID", "positive_culture", "negative_culture", "CHARTTIME"], how = "left").dropna()

cultures = cultures.drop_duplicates()

antibiotics = antibiotics.merge(cultures, left_on = ["SUBJECT_ID", "HADM_ID"], right_on = ["SUBJECT_ID", "HADM_ID"], how = "left")

antibiotics["culture_time"] = antibiotics["CHARTTIME"]

antibiotics = antibiotics[["SUBJECT_ID", "HADM_ID", "positive_culture", "negative_culture", "culture_time"]]
antibiotics["positive_culture"] = [1 if a == 1 else 0 for a in antibiotics["positive_culture"]]
antibiotics["negative_culture"] = [1 if a == 1 else 0 for a in antibiotics["negative_culture"]]
antibiotics["antibiotics_prescribed"] = [1] * len(antibiotics.index)

antibiotics.to_csv("../data/antibiotics_culture.csv", index = False)
