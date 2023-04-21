"""Because no one expects the Spanish Inquisition!"""

import boto3
import json
from botocore.config import Config
import time
import random
import sys, traceback
from timeit import default_timer as timer
import numpy as np
import datetime
import pandas as pd
import os
from collections import defaultdict, namedtuple
import argparse

'''
## Create a timestream query client.
'''
def create_query_client(region, profile = None):
    if profile == None:
        print("Using credentials from the environment")

    print(region)
    config = Config()
    if profile != None:
        session = boto3.Session(profile_name = profile)
        client = session.client(service_name = 'timestream-query',
                                region_name = region, config = config)
    else:
        session = boto3.Session()
        client = session.client(service_name = 'timestream-query',
                                region_name = region, config = config)

    return client

def parse_datum(c_type, data):
    if ('ScalarType' in c_type):
        return parse_scalar(c_type['ScalarType'], data.get('ScalarValue'))
    elif ('ArrayColumnInfo' in c_type):
        return parse_array_data(c_type['ArrayColumnInfo'], data.get('ArrayValue'))
    elif ('TimeSeriesMeasureValueColumnInfo' in c_type):
        return parse_ts_data(c_type['TimeSeriesMeasureValueColumnInfo'], data.get('TimeSeriesValue'))
    elif ('RowColumnInfo' in c_type):
        return parse_row_data(c_type['RowColumnInfo'], data.get('RowValue'))
    else:
        raise Exception("All the data is Null???")

def parse_scalar(c_type, data):
    if data == None:
        return None
    if (c_type == "VARCHAR"):
        return data
    elif (c_type == "BIGINT"):
        return int(data)
    elif (c_type == "DOUBLE"):
        return float(data)
    elif (c_type == "INTEGER"):
        return int(data)
    elif (c_type == "BOOLEAN"):
        return bool(data)
    elif (c_type == "TIMESTAMP"):
        return data
    else:
        return data

def parse_array_data(c_type, data):
    if data == None:
        return None
    datum_list = []
    for elem in data:
        datum_list.append(parse_datum(c_type['Type'], elem))
    return datum_list

def parse_ts_data(c_type, data):
    if data == None:
        return None
    datum_list = []
    for elem in data:
        ts_data = {}
        ts_data['time'] = elem['Time']
        ts_data['value'] = parse_datum(c_type['Type'], elem['Value'])
        datum_list.append(ts_data)
    return datum_list

def parse_row_data(c_types, data):
    if data == None:
        return None
    datum_dict = {}
    for c_type, elem in zip(c_types, data['Data']):
        datum_dict[c_type['Name']] = parse_datum(c_type['Type'], elem)
    return datum_dict

def flat_model_to_dataframe(items):
    """
    Translate a Timestream query SDK result into a Pandas dataframe.
    """
    return_val = defaultdict(list)
    for obj in items:
        for row in obj.get('Rows'):
            for c_info, data in zip(obj['ColumnInfo'], row['Data']):
                c_name = c_info['Name']
                c_type = c_info['Type']
                return_val[c_name].append(parse_datum(c_type, data))

    df = pd.DataFrame(return_val)
    return df

def execute_query_and_return_as_dataframe(client, query, timing = False, log_file = None):
    return flat_model_to_dataframe(execute_query(client, query, timing, log_file))

def execute_query(client, query, timing = False, log_file = None):
    try:
        pages = None
        query_id = None
        first_result = None
        start = timer()
        ## Create the paginator to paginate through the results.
        paginator = client.get_paginator('query')
        page_iterator = paginator.paginate(QueryString=query)
        empty_pages = 0
        pages = list()
        last_page = None
        for page in page_iterator:
            if 'QueryId' in page and query_id == None:
                query_id = page['QueryId']
                print("QueryId: {}".format(query_id))

            last_page = page

            if 'Rows' not in page or len(page['Rows']) == 0:
                ## We got an empty page.
                empty_pages +=1
            else:
                pages.append(page)
                if first_result == None:
                    ## Note the time when the first row of result was received.
                    first_result = timer()

        ## If there were no result, then return the last empty page to carry over the query results context
        if len(pages) == 0 and last_page != None:
            pages.append(last_page)
        return pages
    except Exception as e:
        if query_id != None:
            ## Try canceling the query if it is still running
            print("Attempting to cancel query: {}".format(query_id))
            try:
                client.cancel_query(query_id=query_id)
            except:
                pass
        print(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
        if e.response != None:
            query_id = None
            print("RequestId: {}".format(e.response['ResponseMetadata']['RequestId']))
            if 'QueryId' in e.response:
                query_id = e.response['QueryId']
            print("QueryId: {}".format(query_id))
        raise e
    except KeyboardInterrupt:
        if query_id != None:
            ## Try canceling the query if it is still running
            print("Attempting to cancel query: {}".format(query_id))
            try:
                client.cancel_query(query_id=query_id)
            except:
                pass
        raise
    finally:
        end = timer()
        if timing == True:
            now = datetime.datetime.utcnow()
            if first_result != None:
                time_to_first_result = first_result - start
                time_to_read_results = end - first_result
            else:
                time_to_first_result = end - start
                time_to_read_results = 0

            timing_msg = "{}. QueryId: {} Time: {}. First result: {}. Time to read results: {}.".format(now.strftime("%Y-%m-%d %H:%M:%S"),
                                                                                                       query_id, round(end - start, 3), round(time_to_first_result, 3), round(time_to_read_results, 3))
            print(timing_msg)
            bytes_scanned = pages[-1]['QueryStatus']['CumulativeBytesScanned']
            bytes_per_GB = 1000 * 1000 * 1000
            # cost is 0.01 per GB scanned, report a minimum of 0.01 to avoid confusion
            query_cost = max(round((bytes_scanned / bytes_per_GB) * 0.01, 3), 0.01)
            print("Est. Query cost: $", query_cost)
            if log_file != None:
                log_file.write("{}\n".format(timing_msg))
