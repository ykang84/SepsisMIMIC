import pandas as pd
import numbers
import scipy.stats as stats

"""
for code, description in zip(list(rv["ITEMID"]), list(rv["LABEL"])):
    tempFrame = rv_events[["ITEMID", "VALUE", "VALUEUOM"]][rv_events["ITEMID"] == code]
    print("\n" + description)
    print(tempFrame[["ITEMID", "VALUEUOM"]].drop_duplicates())
    print("summary: ")
    print(tempFrame["VALUE"].describe())
    print("min max mean median")
    try:
        print(tempFrame.VALUE.astype(float).min(skipna = True), tempFrame.VALUE.astype(float).max(skipna = True), tempFrame.VALUE.astype(float).mean(skipna = True), tempFrame.VALUE.astype(float).median(skipna = True))
    except:
        continue
"""

def get_data_summary(rv_e, features):
    data_summary = [[]]
    for f in features:
        print(f)
        print(rv_e[rv_e["ITEMID"] == f][["VALUE"]].dropna(axis = "rows").mean())
        data_summary += [[f, rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows").mean()]]
    return(data_summary)

def clean_features(rv_e, features):
    feature_dict = dict()
    for f in features:
        if  f == 224166: #Doppler BP (diastolic)
            feature_dict["Doppler_BP"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Doppler_BP"]["VALUE"] = feature_dict["Doppler_BP"].VALUE.astype(float)
            feature_dict["Doppler_BP"] = feature_dict["Doppler_BP"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Doppler_BP"]["VALUE"] >= 20]
         
        elif f == 224167: #Manual Blood Pressure Systolic Left
            feature_dict["Manual_BP_Systolic_Left"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Manual_BP_Systolic_Left"]["VALUE"] = feature_dict["Manual_BP_Systolic_Left"].VALUE.astype(float)
            feature_dict["Manual_BP_Systolic_Left"] = feature_dict["Manual_BP_Systolic_Left"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Manual_BP_Systolic_Left"]["VALUE"] >= 20]
        
        elif f == 224192: #Pulsus Paradoxus
            feature_dict["Pulsus_Paradoxus"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Pulsus_Paradoxus"]["VALUE"] = feature_dict["Pulsus_Paradoxus"].VALUE.astype(float)
            feature_dict["Pulsus_Paradoxus"] = feature_dict["Pulsus_Paradoxus"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Pulsus_Paradoxus"]["VALUE"] > 0]
        
        elif f == 227242: #Manual Blood Pressure Diastolic Right
            feature_dict["Manual_BP_Diastolic_Right"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Manual_BP_Diastolic_Right"]["VALUE"] = feature_dict["Manual_BP_Diastolic_Right"].VALUE.astype(float)
            feature_dict["Manual_BP_Diastolic_Right"] = feature_dict["Manual_BP_Diastolic_Right"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Manual_BP_Diastolic_Right"]["VALUE"] >= 20]

        elif f == 224359: #QTc
            feature_dict["QTc"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["QTc"]["VALUE"] = feature_dict["QTc"].VALUE.astype(float).map(lambda x: x if x < 1 else x / 10 if x < 10 else x / 100 if x < 100 else x / 1000)
            feature_dict["QTc"] = feature_dict["QTc"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["QTc"]["VALUE"] > 0]
        
        elif f == 227243: #Manual Blood Pressure Systolic Right
            feature_dict["Manual_BP_Systolic_Right"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Manual_BP_Systolic_Right"]["VALUE"] = feature_dict["Manual_BP_Systolic_Right"].VALUE.astype(float)
            feature_dict["Manual_BP_Systolic_Right"] = feature_dict["Manual_BP_Systolic_Right"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Manual_BP_Systolic_Right"]["VALUE"] >= 20]
    
        elif f == 227634: #Arctic Sun Temp #2 C
            feature_dict["Arctic_Sun_Temp_2_C"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Arctic_Sun_Temp_2_C"]["VALUE"] = feature_dict["Arctic_Sun_Temp_2_C"].VALUE.astype(float).map(lambda x: x if x < 43 and x > 25 else (x - 32) * 5 / 9 if x >= 43 else x)        
            feature_dict["Arctic_Sun_Temp_2_C"] = feature_dict["Arctic_Sun_Temp_2_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arctic_Sun_Temp_2_C"]["VALUE"] >= 30]
            feature_dict["Arctic_Sun_Temp_2_C"] = feature_dict["Arctic_Sun_Temp_2_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arctic_Sun_Temp_2_C"]["VALUE"] <= 43]

        elif f == 227632: #Arctic Sun Temp #1 C
            feature_dict["Arctic_Sun_Temp_1_C"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Arctic_Sun_Temp_1_C"]["VALUE"] = feature_dict["Arctic_Sun_Temp_1_C"].VALUE.astype(float).map(lambda x: x if x < 43 and x > 25 else (x - 32) * 5 / 9 if x >= 43 else x)              
            feature_dict["Arctic_Sun_Temp_1_C"] = feature_dict["Arctic_Sun_Temp_1_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arctic_Sun_Temp_1_C"]["VALUE"] >= 30]
            feature_dict["Arctic_Sun_Temp_1_C"] = feature_dict["Arctic_Sun_Temp_1_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arctic_Sun_Temp_1_C"]["VALUE"] <= 43]

        elif f == 224643: #Manual Blood Pressure Diastolic Left
            feature_dict["Manual_BP_Diastolic_Left"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Manual_BP_Diastolic_Left"]["VALUE"] = feature_dict["Manual_BP_Diastolic_Left"].VALUE.astype(float)
            feature_dict["Manual_BP_Diastolic_Left"] = feature_dict["Manual_BP_Diastolic_Left"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Manual_BP_Diastolic_Left"]["VALUE"] >= 20]

        elif f == 224645: #Orthostatic BPs lying
            feature_dict["Ortho_BP_systolic_lying"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_BP_systolic_lying"]["VALUE"] = feature_dict["Ortho_BP_systolic_lying"].VALUE.astype(float)
            feature_dict["Ortho_BP_systolic_lying"] = feature_dict["Ortho_BP_systolic_lying"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_BP_systolic_lying"]["VALUE"] >= 20]

        elif f == 224646: #Orthostatic BPs sitting (systolic)
            feature_dict["Ortho_BP_systolic_sitting"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_BP_systolic_sitting"]["VALUE"] = feature_dict["Ortho_BP_systolic_sitting"].VALUE.astype(float)
            feature_dict["Ortho_BP_systolic_sitting"] = feature_dict["Ortho_BP_systolic_sitting"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_BP_systolic_sitting"]["VALUE"] >= 20]

        elif f == 224647: #Orthostatic HR standing
            feature_dict["Ortho_HR_standing"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_HR_standing"]["VALUE"] = feature_dict["Ortho_HR_standing"].VALUE.astype(float)
            feature_dict["Ortho_HR_standing"] = feature_dict["Ortho_HR_standing"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_standing"]["VALUE"] >= 10]

        elif f == 226096: #Orthostatic BPd standing (diastolic)
            feature_dict["Ortho_BP_diastolic_standing"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_BP_diastolic_standing"]["VALUE"] = feature_dict["Ortho_BP_diastolic_standing"].VALUE.astype(float)
            feature_dict["Ortho_BP_diastolic_standing"] = feature_dict["Ortho_BP_diastolic_standing"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_BP_diastolic_standing"]["VALUE"] >= 20]

        elif f == 220045: #Heart Rate
            feature_dict["HR"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["HR"]["VALUE"] = feature_dict["HR"].VALUE.astype(float)
            feature_dict["HR"] = feature_dict["HR"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["HR"]["VALUE"] >= 10]
            feature_dict["HR"] = feature_dict["HR"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["HR"]["VALUE"] <= 200]

        elif f == 220050: #Arterial Blood Pressure Systolic
            feature_dict["Arterial_BP_systolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Arterial_BP_systolic"]["VALUE"] = feature_dict["Arterial_BP_systolic"].VALUE.astype(float)
            feature_dict["Arterial_BP_systolic"] = feature_dict["Arterial_BP_systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_systolic"]["VALUE"] >= 20]
            feature_dict["Arterial_BP_systolic"] = feature_dict["Arterial_BP_systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_systolic"]["VALUE"] <= 250]

        elif f == 220051: #Arterial Blood Pressure Diastolic
            feature_dict["Arterial_BP_diastolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Arterial_BP_diastolic"]["VALUE"] = feature_dict["Arterial_BP_diastolic"].VALUE.astype(float)
            feature_dict["Arterial_BP_diastolic"] = feature_dict["Arterial_BP_diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_diastolic"]["VALUE"] >= 20]
            feature_dict["Arterial_BP_diastolic"] = feature_dict["Arterial_BP_diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_diastolic"]["VALUE"] <= 250]

        elif f == 220052: #Arterial Blood Pressure Mean
            feature_dict["Arterial_BP_mean"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Arterial_BP_mean"]["VALUE"] = feature_dict["Arterial_BP_mean"].VALUE.astype(float)
            feature_dict["Arterial_BP_mean"] = feature_dict["Arterial_BP_mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_mean"]["VALUE"] >= 20]
            feature_dict["Arterial_BP_mean"] = feature_dict["Arterial_BP_mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Arterial_BP_mean"]["VALUE"] <= 200]

        elif f == 220179: #Non Invasive Blood Pressure systolic
            feature_dict["Noninvasive_BP_systolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Noninvasive_BP_systolic"]["VALUE"] = feature_dict["Noninvasive_BP_systolic"].VALUE.astype(float)
            feature_dict["Noninvasive_BP_systolic"] = feature_dict["Noninvasive_BP_systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_systolic"]["VALUE"] >= 20]
            feature_dict["Noninvasive_BP_systolic"] = feature_dict["Noninvasive_BP_systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_systolic"]["VALUE"] <= 250]

        elif f == 220180: #Non Invasive Blood Pressure diastolic
            feature_dict["Noninvasive_BP_diastolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Noninvasive_BP_diastolic"]["VALUE"] = feature_dict["Noninvasive_BP_diastolic"].VALUE.astype(float)
            feature_dict["Noninvasive_BP_diastolic"] = feature_dict["Noninvasive_BP_diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_diastolic"]["VALUE"] >= 20]
            feature_dict["Noninvasive_BP_diastolic"] = feature_dict["Noninvasive_BP_diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_diastolic"]["VALUE"] <= 250]

        elif f == 220181: #Non Invasive Blood Pressure mean
            feature_dict["Noninvasive_BP_mean"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Noninvasive_BP_mean"]["VALUE"] = feature_dict["Noninvasive_BP_mean"].VALUE.astype(float)
            feature_dict["Noninvasive_BP_mean"] = feature_dict["Noninvasive_BP_mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_mean"]["VALUE"] >= 20]
            feature_dict["Noninvasive_BP_mean"] = feature_dict["Noninvasive_BP_mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Noninvasive_BP_mean"]["VALUE"] <= 200]

        elif f == 226094: #Orthostatic BPd sitting (diastolic)
            feature_dict["Ortho_BP_diastolic_sitting"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_BP_diastolic_sitting"]["VALUE"] = feature_dict["Ortho_BP_diastolic_sitting"].VALUE.astype(float)
            feature_dict["Ortho_BP_diastolic_sitting"] = feature_dict["Ortho_BP_diastolic_sitting"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_BP_diastolic_sitting"]["VALUE"] >= 20]

        elif f == 226092: #Orthostatic BPd lying (diastolic)
            feature_dict["Ortho_BP_diastolic_lying"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_BP_diastolic_lying"]["VALUE"] = feature_dict["Ortho_BP_diastolic_lying"].VALUE.astype(float)
            feature_dict["Ortho_BP_diastolic_lying"] = feature_dict["Ortho_BP_diastolic_lying"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_BP_diastolic_lying"]["VALUE"] >= 20]

        elif f == 226329: #Blood Temperature CCO (C)
            feature_dict["Blood_Temp_CCO_C"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Blood_Temp_CCO_C"]["VALUE"] = feature_dict["Blood_Temp_CCO_C"].VALUE.astype(float).map(lambda x: x if x < 45 else (x - 32) * 5 / 9)
            feature_dict["Blood_Temp_CCO_C"] = feature_dict["Blood_Temp_CCO_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Blood_Temp_CCO_C"]["VALUE"] >= 30]
            feature_dict["Blood_Temp_CCO_C"] = feature_dict["Blood_Temp_CCO_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Blood_Temp_CCO_C"]["VALUE"] < 44]

        elif f == 223761: #Temperature Fahrenheit (C)
            feature_dict["Temp_Fahrenheit_C"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Temp_Fahrenheit_C"]["VALUE"] = feature_dict["Temp_Fahrenheit_C"].VALUE.astype(float)
            feature_dict["Temp_Fahrenheit_C"]["VALUE"] = feature_dict["Temp_Fahrenheit_C"]["VALUE"].map(lambda x: x if x >= 70 and x <= 108 else x * 9 / 5 + 32)
            feature_dict["Temp_Fahrenheit_C"] = feature_dict["Temp_Fahrenheit_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Temp_Fahrenheit_C"]["VALUE"] >= 70]
            feature_dict["Temp_Fahrenheit_C"] = feature_dict["Temp_Fahrenheit_C"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Temp_Fahrenheit_C"]["VALUE"] <= 108]
            feature_dict["Temp_Fahrenheit_C"]["VALUE"] = feature_dict["Temp_Fahrenheit_C"]["VALUE"].map(lambda x: (x - 32) * 5 / 9)

        elif f == 223762: #Temperature Celsius
            feature_dict["Temp_Celsius"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Temp_Celsius"]["VALUE"] = feature_dict["Temp_Celsius"].VALUE.astype(float).map(lambda x: x if x < 45 and x > 30 else (x - 32) * 5 / 9)
            feature_dict["Temp_Celsius"] = feature_dict["Temp_Celsius"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Temp_Celsius"]["VALUE"] >= 30]
            feature_dict["Temp_Celsius"] = feature_dict["Temp_Celsius"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Temp_Celsius"]["VALUE"] < 44]

        elif f == 223763: #Bladder Pressure
            feature_dict["Bladder_Pressure"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Bladder_Pressure"]["VALUE"] = feature_dict["Bladder_Pressure"].VALUE.astype(float)
            feature_dict["Bladder_Pressure"] = feature_dict["Bladder_Pressure"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Bladder_Pressure"]["VALUE"] <= 80]

        elif f == 223764: #Orthostatic HR Lying
            feature_dict["Ortho_HR_lying"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_HR_lying"]["VALUE"] = feature_dict["Ortho_HR_lying"].VALUE.astype(float)
            feature_dict["Ortho_HR_lying"] = feature_dict["Ortho_HR_lying"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_lying"]["VALUE"] >= 10]
            feature_dict["Ortho_HR_lying"] = feature_dict["Ortho_HR_lying"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_lying"]["VALUE"] <= 200]

        elif f == 223765: #Orthostatic HR Sitting
            feature_dict["Ortho_HR_sitting"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_HR_sitting"]["VALUE"] = feature_dict["Ortho_HR_sitting"].VALUE.astype(float)
            feature_dict["Ortho_HR_sitting"] = feature_dict["Ortho_HR_sitting"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_sitting"]["VALUE"] >= 10]
            feature_dict["Ortho_HR_sitting"] = feature_dict["Ortho_HR_sitting"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_sitting"]["VALUE"] <= 200]

        elif f == 223766: #Orthostatic HR Standing
            feature_dict["Ortho_HR_standing"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["Ortho_HR_standing"]["VALUE"] = feature_dict["Ortho_HR_standing"].VALUE.astype(float)
            feature_dict["Ortho_HR_standing"] = feature_dict["Ortho_HR_standing"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_standing"]["VALUE"] >= 10]
            feature_dict["Ortho_HR_standing"] = feature_dict["Ortho_HR_standing"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["Ortho_HR_standing"]["VALUE"] <= 200]

        elif f == 225309: #ART BP Systolic
            feature_dict["ART_BP_Systolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["ART_BP_Systolic"]["VALUE"] = feature_dict["ART_BP_Systolic"].VALUE.astype(float)
            feature_dict["ART_BP_Systolic"] = feature_dict["ART_BP_Systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Systolic"]["VALUE"] >= 20]
            feature_dict["ART_BP_Systolic"] = feature_dict["ART_BP_Systolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Systolic"]["VALUE"] <= 250]

        elif f == 225310: #ART BP Diastolic
            feature_dict["ART_BP_Diastolic"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["ART_BP_Diastolic"]["VALUE"] = feature_dict["ART_BP_Diastolic"].VALUE.astype(float)
            feature_dict["ART_BP_Diastolic"] = feature_dict["ART_BP_Diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Diastolic"]["VALUE"] >= 20]
            feature_dict["ART_BP_Diastolic"] = feature_dict["ART_BP_Diastolic"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Diastolic"]["VALUE"] <= 200]

        elif f == 225312: #ART BP Mean
            feature_dict["ART_BP_Mean"] = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            feature_dict["ART_BP_Mean"]["VALUE"] = feature_dict["ART_BP_Mean"].VALUE.astype(float)
            feature_dict["ART_BP_Mean"] = feature_dict["ART_BP_Mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Mean"]["VALUE"] >= 20]
            feature_dict["ART_BP_Mean"] = feature_dict["ART_BP_Mean"][["SUBJECT_ID", "HADM_ID", "CHARTTIME", "VALUE"]][feature_dict["ART_BP_Mean"]["VALUE"] <= 200]

        # Now to create dummy variables for categorical features
        # For any categorical features, we only create a dummy if there are more than 100 occurrences of that feature
        elif f == 227630: #Arctic Sun Temp #1 Location
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Arctic_Sun_Temp_1_Loc_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Arctic_Sun_Temp_1_Loc_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Arctic_Sun_Temp_1_Loc_" + val.replace(" ", "_")].index)

        elif f == 227631: #Arctic Sun Temp #2 Location
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Arctic_Sun_Temp_2_Loc_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Arctic_Sun_Temp_2_Loc_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Arctic_Sun_Temp_2_Loc_" + val.replace(" ", "_")].index)

        elif f == 224642: #Temperature Site
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Temperature_Site_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Temperature_Site_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Temperature_Site_" + val.replace(" ", "_")].index)

        elif f == 224650: #Ectopy Type 1
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Ectopy_Type_1_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Ectopy_Type_1_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Ectopy_Type_1_" + val.replace(" ", "_")].index)

        elif f == 224651: #Ectopy Frequency 1
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Ectopy_Frequency_1_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Ectopy_Frequency_1_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Ectopy_Frequency_1_" + val.replace(" ", "_")].index)

        elif f == 228229: #PAR-Activity
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Activity_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Activity_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Activity_" + val.replace(" ", "_")].index)

        elif f == 228230: #PAR-Circulation
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Circulation_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Circulation_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Circulation_" + val.replace(" ", "_")].index)

        elif f == 228231: #PAR-Consciousness
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Consciousness_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Consciousness_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Consciousness_" + val.replace(" ", "_")].index)

        elif f == 228232: #PAR-Oxygen Saturation
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Oxygen_Saturation_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Oxygen_Saturation_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Oxygen_Saturation_" + val.replace(" ", "_")].index)

        elif f == 228233: #PAR-Remain sedated
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Remain_sedated_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Remain_sedated_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Remain_sedated_" + val.replace(" ", "_")].index)

        elif f == 228234: #PAR-Respiration
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["PAR_Respiration_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["PAR_Respiration_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["PAR_Respiration_" + val.replace(" ", "_")].index)

        elif f == 220048: #Heart Rhythm
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Heart_Rhythm_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Heart_Rhythm_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Heart_Rhythm_" + val.replace(" ", "_")].index)

        elif f == 226479: #Ectopy Type 2
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Ectopy_Type_2_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Ectopy_Type_2_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Ectopy_Type_2_" + val.replace(" ", "_")].index)

        elif f == 226480: #Ectopy Frequency 2
            temp = rv_e[rv_e["ITEMID"] == f].dropna(axis = "rows")
            values = temp.VALUE.unique()
            for val in values:
                if len(temp[temp["VALUE"] == val].index) > 100:
                    feature_dict["Ectopy_Frequency_2_" + val.replace(" ", "_")] = temp[temp["VALUE"] == val][["SUBJECT_ID", "HADM_ID", "CHARTTIME"]]
                    feature_dict["Ectopy_Frequency_2_" + val.replace(" ", "_")]["VALUE"] = [1] * len(feature_dict["Ectopy_Frequency_2_" + val.replace(" ", "_")].index)
                    #print("Ectopy_Frequency_2_" + val.replace(" ", "_"), len(feature_dict["Ectopy_Frequency_2_" + val.replace(" ", "_")].index))
    return(feature_dict)

def check_correlations(feature_dict):
    sepsis_data = pd.read_csv("../data/sepsis_labeled_martin.csv", low_memory = False)
    #If the feature has a count less than 100, remove it, else if less than 200 and corr 
    #less than 0.05, remove it, else if less than 1000 and corr less than 0.01, remove it.
    features_to_delete = []
    for k in feature_dict:
        temp = feature_dict[k]
        temp = temp.merge(sepsis_data, left_on = "SUBJECT_ID", right_on = "subject_id", how = "outer").fillna(0)
        # If the feature is a dummy feature, consider correlation for all the rows; otherwise
        # consider only the correlation for rows that have a value for the feature.
        cor = round(temp.fillna(0)["VALUE"].corr(temp.fillna(0)["sepsis"]), 5) if len(temp.fillna(0).VALUE.unique()) == 2 else round(temp[["VALUE", "sepsis"]].dropna(axis = "rows").VALUE.corr(temp[["VALUE", "sepsis"]].dropna(axis = "rows").sepsis), 5)
        count = len(feature_dict[k].index)
        print(k, "count: " + str(count), "corr: " + str(cor))
        if count < 100:
            features_to_delete += [k]
            print("Deleting " + k)
        elif count < 200 and abs(cor) < 0.05:
            features_to_delete += [k]
            print("Deleting " + k)
        elif count < 1000 and abs(cor) < 0.01:
            features_to_delete += [k]
            print("Deleting " + k)
    features_to_delete = [a for a in features_to_delete if a != ""]
    print("Deleting " + str(len(features_to_delete)))
    print(features_to_delete)
    for f in features_to_delete:
        del feature_dict[f]
    return(feature_dict)

def combine_features(feature_dict):
    print("Before combining, there were " + str(len(feature_dict.keys())) + " features.")
    combos = [["Ortho_HR_standing", "Ortho_HR_lying", "Ortho_HR_sitting"], ["Arctic_Sun_Temp_2_C", "Arctic_Sun_Temp_1_C", "Blood_Temp_CCO_C", "Temp_Fahrenheit_C", "Temp_Celsius"], ["Manual_BP_Systolic_Left", "Manual_BP_Systolic_Right", "Ortho_BP_systolic_lying", "Ortho_BP_systolic_sitting", "Arterial_BP_systolic", "Noninvasive_BP_systolic", "ART_BP_Systolic"], ["Doppler_BP", "Manual_BP_Diastolic_Right", "Manual_BP_Diastolic_Left", "Ortho_BP_diastolic_standing", "Arterial_BP_diastolic", "Noninvasive_BP_diastolic", "Ortho_BP_diastolic_sitting", "Ortho_BP_diastolic_lying", "ART_BP_Diastolic"], ["Arterial_BP_mean", "Noninvasive_BP_mean", "ART_BP_Mean"]]
    for c in combos:
        combined_feat = feature_dict[c[0]]
        for f in c[1:]:
            combined_feat = combined_feat.append(feature_dict[f])
        print("Total unique patients: " + str(len(set(combined_feat["SUBJECT_ID"]))))
        for f in c:
            print(f, len(feature_dict[f].index))
            print(feature_dict[f].VALUE.astype(float).min(), feature_dict[f].VALUE.astype(float).max(), feature_dict[f].VALUE.astype(float).mean(), feature_dict[f].VALUE.astype(float).median())
        f, p = stats.f_oneway(*[list(feature_dict[f].VALUE) for f in c])
        print("F-test: " + str(p))
        if "Ortho_HR_sitting" in c and "Ortho_HR_lying" in c:
            # merge lying and sitting
            feature_dict["Ortho_HR_lying_sitting"] = feature_dict["Ortho_HR_sitting"].append(feature_dict["Ortho_HR_lying"])
            del feature_dict["Ortho_HR_lying"]
            del feature_dict["Ortho_HR_sitting"]
        elif "Arctic_Sun_Temp_2_C" in c and "Arctic_Sun_Temp_1_C" in c:
            #print(stats.ttest_ind(feature_dict["Arctic_Sun_Temp_1_C"].VALUE, feature_dict["Arctic_Sun_Temp_2_C"].VALUE))
            #print(stats.ttest_ind(feature_dict["Blood_Temp_CCO_C"].VALUE, feature_dict["Temp_Fahrenheit_C"].VALUE))
            #print(stats.ttest_ind(feature_dict["Temp_Celsius"].VALUE, feature_dict["Temp_Fahrenheit_C"].VALUE))
            #Only temp f and temp c can go together
            feature_dict["Temp_C_F"] = feature_dict["Temp_Fahrenheit_C"].append(feature_dict["Temp_Celsius"])
            del feature_dict["Temp_Fahrenheit_C"]
            del feature_dict["Temp_Celsius"]
        elif "Manual_BP_Systolic_Left" in c and "Manual_BP_Systolic_Right" in c:
            #print(stats.ttest_ind(feature_dict["Manual_BP_Systolic_Left"].VALUE, feature_dict["Manual_BP_Systolic_Right"].VALUE))
            #print(stats.ttest_ind(feature_dict["Arterial_BP_systolic"].VALUE, feature_dict["ART_BP_Systolic"].VALUE))
            #f, p = stats.f_oneway(feature_dict["Manual_BP_Systolic_Left"].VALUE, feature_dict["Manual_BP_Systolic_Right"].VALUE, feature_dict["Noninvasive_BP_systolic"].VALUE)
            #print(p)
            feature_dict["Manual_Noninvasive_BP_Systolic"] = feature_dict["Manual_BP_Systolic_Left"].append(feature_dict["Manual_BP_Systolic_Right"]).append(feature_dict["Noninvasive_BP_systolic"])
            del feature_dict["Manual_BP_Systolic_Left"]
            del feature_dict["Manual_BP_Systolic_Right"]
            del feature_dict["Noninvasive_BP_systolic"]
        elif "Ortho_BP_diastolic_standing" in c and "Ortho_BP_diastolic_sitting" in c:
            #print(stats.f_oneway(feature_dict["Ortho_BP_diastolic_sitting"].VALUE, feature_dict["Ortho_BP_diastolic_standing"].VALUE, feature_dict["Noninvasive_BP_diastolic"].VALUE)[1])
            #f, p = stats.f_oneway(feature_dict["Ortho_BP_diastolic_standing"].VALUE, feature_dict["Ortho_BP_diastolic_sitting"].VALUE, feature_dict["Ortho_BP_diastolic_lying"].VALUE)
            #print(p)
            #f, p = stats.f_oneway(feature_dict["Ortho_BP_diastolic_standing"].VALUE, feature_dict["Ortho_BP_diastolic_sitting"].VALUE, feature_dict["Ortho_BP_diastolic_lying"].VALUE, feature_dict["Ortho_BP_diastolic_sitting"].VALUE, feature_dict["Ortho_BP_diastolic_standing"].VALUE, feature_dict["Noninvasive_BP_diastolic"].VALUE)
            #print(p)
            feature_dict["Manual_BP_Noninvasive_Diastolic"] = feature_dict["Manual_BP_Diastolic_Left"].append(feature_dict["Noninvasive_BP_diastolic"]).append(feature_dict["Manual_BP_Diastolic_Right"]).append(feature_dict["Ortho_BP_diastolic_standing"]).append(feature_dict["Ortho_BP_diastolic_lying"]).append(feature_dict["Ortho_BP_diastolic_sitting"])
            feature_dict["Ortho_BP_diastolic"] = feature_dict["Ortho_BP_diastolic_standing"].append(feature_dict["Ortho_BP_diastolic_lying"]).append(feature_dict["Ortho_BP_diastolic_sitting"])
            del feature_dict["Ortho_BP_diastolic_standing"]
            del feature_dict["Ortho_BP_diastolic_lying"]
            del feature_dict["Ortho_BP_diastolic_sitting"]
            del feature_dict["Manual_BP_Diastolic_Left"]
            del feature_dict["Noninvasive_BP_diastolic"]
            del feature_dict["Manual_BP_Diastolic_Right"]
        print(" ")
    feature_dict = check_correlations(feature_dict)
    print("There are now " + str(len(feature_dict.keys())) + " features.")
    return(feature_dict)

def main():
    rv_events = pd.read_csv("routine_vitals_events.csv", low_memory = False)
    rv = pd.read_csv("routine_vitals_signs.csv")
    features = list(rv["ITEMID"])
    rv_e = rv_events[["SUBJECT_ID", "HADM_ID", "ITEMID", "CHARTTIME", "VALUE"]]
    feature_dict = clean_features(rv_e, features)
    print("After cleaning features, there are now " + str(len(feature_dict.keys())) + " features.")
    feature_dict = combine_features(feature_dict)
    print("After combining features, there are now " + str(len(feature_dict.keys())) + " features.")
    feature_keys = list(feature_dict.keys())
    f = feature_dict[feature_keys[0]]
    f["ITEMID"] = [feature_keys[0]] * len(f.index)
    output_dataframe = f
    for feature in feature_keys[1:]:
        f = feature_dict[feature]
        f["ITEMID"] = [feature] * len(f.index)
        output_dataframe = output_dataframe.append(f)
    output_dataframe.to_csv("../data/ETL_output.csv", index = False)
        
main()
