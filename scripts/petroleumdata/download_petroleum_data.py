#
# Script to acquire the petroleum spot price data from the gov API
# Run script with a single command line argument that should be the API key
#
# The script makes a request to the API, parses the data and writes to a .csv file
# that we can then put on DUMBO
#

import requests
import sys
import csv

API_KEY = sys.argv[1]
base_url = "http://api.eia.gov/series/"

# TODO: will extend to allow for download granularity
series_map = {'daily': 'PET.RWTC.D', 'weekly': 'PET.RWTC.M', 'yearly': 'PET.RWTC.A', }

#
# Build query string for API request
#
def build_API_string(base_url, API_KEY):
    string = base_url + "?api_key=" + API_KEY + "&series_id=PET.RWTC.D"
    return string

#
# Build query string with limited range
#
def limit_range(url_string, start_date, end_date):
    string = url_string + "&start=" + start_date + "&end=" + end_date
    return string

#
# Pull the raw data for all dates available
#
def pull_data_all():
    string = build_API_string(base_url, API_KEY)
    save_series_to_csv(make_request(string), "all")

#
# Pull the raw data given a range
#
def pull_data_range(start, end):
    string = limit_range(build_API_string(base_url, API_KEY), start, end)
    save_series_to_csv(make_request(string), "from-" + start + "-to-" + end)

#
# Makes a request to the URL to get the raw data. Data is returned as a 2D matrix with 2 columns: date, spot_price
#
def make_request(url):
    response = requests.get(url)
    data = response.json()
    series = data["series"][0]["data"]
    return series

def save_series_to_csv(series, range):
    csv_filename = "petroleum-spot-prices-" + range + ".csv"

    print("starting write to " + csv_filename)
    rows_written = 0

    with open(csv_filename, 'a') as csvFile:
        writer = csv.writer(csvFile)
        for tuple in series:
            writer.writerow([tuple[0], tuple[1]])
            rows_written += 1
    print("wrote " + str(rows_written) + " to " + csv_filename)


if __name__== "__main__":
    pull_data_all()