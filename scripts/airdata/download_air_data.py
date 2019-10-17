#
# Air Data Downloader (EPA)
# Cole Smith
#

import os
import json
import time
import datetime
import urllib.request

import multiprocessing
from joblib import Parallel, delayed

# Span all available data
span = [2019, 0]

# Base URL
base_url = "https://aqs.epa.gov/aqsweb/airdata/"

# Available Gasses and Gas ID
gasses = {
        "ozone": 44201,
        "so2": 42401,
        "co": 42101,
        "no2": 42602
        }

# Available particulates and ID
particulates = {
        "PM2.5_FRM-FEM Mass": 88101,
        "PM2.5_non_FRM-FEM Mass": 88502,
        "PM10_Mass": 81102,
        "PM2.5_Speciation": "SPEC",
        "PM10_Speciation": "PM10SPEC"
        }

# Available toxics and ID
toxics = {
        "HAPs": "HAPS",
        "VOCs": "VOCS",
        "NONOxNOy": "NONOxNOy",
        "Lead": "LEAD"
        }

# Meteorological Information and ID
meteorological = {
        "Winds": "WIND",
        "Temperature": "TEMP",
        "Barometric Pressure": "PRESS",
        "RH and Dewpoint": "RH_DP"
        }

# Time granularity
granularity = "hourly"

# Root folder name (will be created at current working dir)
root = "AIRDATA"

# Metadata about download to write into root folder
meta = {
        "time_downloaded": str(datetime.datetime.now()),
        "time_to_download": -1,
        "gasses": None,
        "particulates": None,
        "toxics": None,
        "meteorological": None,
        "time_granularity": granularity,
        "num_datasets": 0
        }

types = {
        "gasses": gasses,
        "particulates": particulates,
        "toxics": toxics,
        "meteorological": meteorological
        }

def write_meta(download_start_time):
    if not os.path.exists(root):
        return

    for t in types:
        if os.path.exists(os.path.join(root, t)):
            meta[t] = []
            for tt in types[t]:
                if os.path.exists(os.path.join(root, t, tt)):
                    meta[t].append(tt)
                    n = len(os.listdir(os.path.join(root, t, tt)))
                    meta['num_datasets'] = meta['num_datasets'] + n

    meta["time_to_download"] = round(time.time() - download_start_time, 2)

    with open(str(os.path.join(root, "meta.json")), 'w') as fp:
        json.dump(meta, fp, sort_keys=True, indent=4)

# -------------------------------------------------------------------------
# --- Downloader
# -------------------------------------------------------------------------

def download(folder_name, subtype_dict):
    # for (folder_name, subtype_dict) in types.items():

    folder_path = os.path.join(root, folder_name)
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

    # meta[folder_name] = []


    for (subfolder_name, data_id) in subtype_dict.items():

        subfolder_path = os.path.join(folder_path, subfolder_name)
        if not os.path.exists(subfolder_path):
            os.mkdir(subfolder_path)

        # meta[folder_name].append(subfolder_name)
        # write_meta()

        # Download the datasets for date range
        upper_year = span[0]
        lower_year = span[1]

        if lower_year < 1980:
            lower_year = 1980

        for year in range(lower_year, upper_year + 1):
            file_name = granularity + '_' + str(data_id) + '_' + str(year) + ".zip"
            file_path = os.path.join(subfolder_path, file_name)

            if not os.path.exists(file_path):
                print("Writing: " + str(file_path))
                urllib.request.urlretrieve(base_url + file_name, str(file_path))

            # meta["num_datasets"] = meta["num_datasets"] + 1
            # write_meta()

# -------------------------------------------------------------------------
# --- Parallel Exec
# -------------------------------------------------------------------------

if not os.path.exists(root):
    os.mkdir(root)

print("Beginning Download...")
start_time = time.time()

# Start a parallel download job
Parallel(n_jobs=len(types))(delayed(download)(f, s) for (f, s) in types.items())

print("Download Finished.")
write_meta(start_time)

