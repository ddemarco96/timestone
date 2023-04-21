import re

import pandas as pd
from calendar import monthrange
from constants import DATABASE_NAME

from inquisitor import create_query_client, execute_query, execute_query_and_return_as_dataframe

import boto3
from botocore.exceptions import ClientError


def get_all_ppts(client, table_name="eda"):
    """Get a list of all participants in the database.
    Returns: A list of all participants in the database.
    """
    q_string = f'''SELECT DISTINCT "ppt_id" FROM "{DATABASE_NAME}"."{table_name}"'''
    df = execute_query_and_return_as_dataframe(client, q_string, timing=True)
    return df.ppt_id.values.tolist()

def get_ppt_df(client, ppt_id, table_name="eda", verbose=False):
    """Get a list of all participants in the database.
    Returns: A list of all participants in the database.
    """
    print("Getting data for participant:", ppt_id) if verbose else None
    assert len(ppt_id) < 7
    q_string = f'''
    SELECT "time", "value", "dev_id" 
    FROM "{DATABASE_NAME}"."{table_name}" 
    WHERE "ppt_id" = '{ppt_id}' 
    '''
    df = execute_query_and_return_as_dataframe(client, q_string, timing=True)
    return df

def filter_ppt_list(ppt_list, regex):
    """Filter a list of participants using a regex.
    Parameters: ppt_list (list): A list of participants.
                regex (str): A regex string.
    Returns: A list of participants that match the regex.
    """
    return [ppt for ppt in ppt_list if re.match(regex, ppt)]

def drop_low_values(df, threshold=0.03):
    """Drop all rows in a dataframe if 90% of the measurements in a 10 second window are below the threshold.
    Parameters: df (pandas dataframe): The dataframe to be filtered.
                threshold (float): The threshold value.
    Returns: A filtered dataframe.
    """
    # grab the first and last time from the df
    starting_len = df.shape[0]
    df.time = pd.to_datetime(df.time)
    first_time = df.time.min()
    last_time = df.time.max()

    # add a column for the number of seconds since the first time
    df['seconds_since_first'] = (df.time - first_time).dt.total_seconds()
    # bin the seconds into 10sec windows -- it is MUCH faster to do it this way then to for loop
    df['10s_from_first'] = df['seconds_since_first'] // 10
    grp = df.groupby('10s_from_first')
    # drop all rows where 90% of the values are below the threshold
    df['drop_group'] = grp.value.transform(lambda x: (x < threshold).sum() / len(x) > 0.9)
    df = df.loc[~df.drop_group]

    ending_len = df.shape[0]

    return df, starting_len, ending_len

def get_wear_time_by_day(df):
    """Return a grouped dataframe with the number of minutes of wear time per day, and the corresponding percent"""
    df.time = pd.to_datetime(df.time)
    df['date'] = df.time.dt.date
    grp = df.groupby(['date', 'dev_id']).count()  # number of rows per day
    grp['minutes_worn'] = grp.time / 4 / 60  # number of minutes per day
    grp['percent_worn'] = grp.minutes_worn / 60 / 24  # percent of the day
    return grp.reset_index()

def create_wear_time_summary(ppt_list=[], list_filter=None, output_dir='.', save=True, verbose=False):
    """
    """
    profile_name = 'nocklab'
    region = "us-east-1"

    client = create_query_client(region, profile=profile_name)
    ppt_list = get_all_ppts(client) if not ppt_list else ppt_list
    if list_filter:
        # use regex to filter the list to only ppts that match the filter
        ppt_list = filter_ppt_list(ppt_list, list_filter)

    if len(ppt_list) == 0:
        print("No participants to scan for. IF you used one, double check your regex filter.")
        return None

    summary_df = pd.DataFrame(columns=['ppt_id', 'dev_id', 'date', 'minutes_worn', 'percent_worn'])
    for ppt_id in ppt_list:
        # query all wear data for that participant from Timestream
        df = get_ppt_df(client, ppt_id=ppt_id, table_name="eda")
        df, starting_len, ending_len = drop_low_values(df, threshold=0.03)
        dropped_pct = round((starting_len - ending_len) / starting_len, 2) * 100
        print(f"Participant {ppt_id} had {starting_len - ending_len} ({dropped_pct}%) rows dropped.")

        # summarize the remaining data by day
        grp = get_wear_time_by_day(df)
        grp['ppt_id'] = ppt_id

        # add the summary data to the list
        summary_data = summary_data.append(grp, ignore_index=True)


    if save:
        summary_df.to_csv(f'{output_dir}/wear_time_summary.csv')
    return summary_df
