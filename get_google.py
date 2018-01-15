#!/usr/bin/python
"""
BEFORE RUNNING:
---------------
1. If not already done, enable the Google Sheets API
   and check the quota for your project at
   https://console.developers.google.com/apis/api/sheets
2. Install the Python client library for Google APIs by running
   `pip install --upgrade google-api-python-client`
"""
from pprint import pprint
from googleapiclient import discovery

# TODO: Change placeholder below to generate authentication credentials. See
# https://developers.google.com/sheets/quickstart/python#step_3_set_up_the_sample
#
# Authorize using one of the following scopes:
#     'https://www.googleapis.com/auth/drive'
#     'https://www.googleapis.com/auth/drive.file'
#     'https://www.googleapis.com/auth/drive.readonly'
#     'https://www.googleapis.com/auth/spreadsheets'
#     'https://www.googleapis.com/auth/spreadsheets.readonly'
credentials = None

service = discovery.build('sheets', 'v4', credentials=credentials)

# The spreadsheet to request.
spreadsheet_id = '10DEHOTLvel3brvP-kiZRqgjo8ThhMSRhmYO5QSdYKKE'  # TODO: Update placeholder value.

# The ranges to retrieve from the spreadsheet.
ranges = []  # TODO: Update placeholder value.

# True if grid data should be returned.
# This parameter is ignored if a field mask was set in the request.
include_grid_data = False  # TODO: Update placeholder value.

request = service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=ranges, includeGridData=include_grid_data)
response = request.execute()

# TODO: Change code below to process the `response` dict:
pprint(response)
