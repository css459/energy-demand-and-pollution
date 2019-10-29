# AirData Downloader
## Cole Smith

The following python scritpts will create an `AIRDATA` folder at the current directory
and begin downloading the specified CSV files from the EPA in parallel.

The total parallel download time for the hourly datasets, from 1980 to 2019, for all criteria,
is about 30 minutes to 1 hour on a very fast connection. Slower connections can take hours.

# Adjustment

One may adjust the `time granularity` by adjusting the `granulatiry` variable to either `daily` or `monthly`.

The time span can be changed using the `span` variable.

# Extracting

The files come in as ZIP files. Afrer the downloader is finished, the `unzip_all.py` script will walk the `AIRDATA`
directory and convert each ZIP to a CSV. (WARNING: THIS WILL EXPAND TO OVER 300GB)

The files are now ready to be uploaded to HDFS by uploading the AIRDATA directory.

`hadoop dfs -put AIRDATA AIRDATA`

