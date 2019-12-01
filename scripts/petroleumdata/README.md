# PetroleumPrice Data Downloader
## Andrii Dobroshynskyi

The python script included downloads the petroleum spot price data from the EIA (U.S. Energy Information Administration) 
data portal. You can specify the range of dates (in year, month, date granularity) or download all data available (the current 
default behavior).

# Running script

`$ python dowload_petroleum_data.py <API_KEY>`

The EIA data portal requires registering for an API key to pull the data which we read as command line argument. You 
can change this behavior to be an `ENV` variable instead if you wish, just make sure you parse it correctly and append it
to every request to the EIA API (see code in downloader for how a request URI is built up).

# Saving data

Depending on the range specified in the script the downloader will pull the data into a `.csv` of `PETROLEUM`
or a `.csv` named based on the range one provides, e.g. `petroleum-spot-prices-from-20190101-to-20190201`.

# Moving to HDFS

Since the amount of data is manageable (usually in order of hundred thousand rows), moving the `.csv` to HDFS can be as easy as

`hdfs dfs -put PETROLEUM.csv PETROLEUM`

