#
# Air Data Downloader (EPA)
# Cole Smith
#

import os
import zipfile
from zipfile import BadZipFile
from joblib import Parallel, delayed

ROOT = "AIRDATA/"

def unzip(f_path, dest_path):
    try:
        with zipfile.ZipFile(f_path, 'r') as zr:
            zr.extractall(dest_path)
    except BadZipFile:
        print("Could not extract:", f_path)

for root, dirs, files in os.walk(ROOT):
    if ".zip" in ','.join(files):

        zips = [os.path.join(root, f) for f in files if ".zip" in f]
        Parallel(n_jobs=len(zips))(delayed(unzip)(f, root) for f in zips)

        print(root)
        print(files)
        print("-------------------")

