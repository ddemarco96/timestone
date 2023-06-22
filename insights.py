import os
import re

import pandas as pd
from calendar import monthrange
from constants import DATABASE_NAME

from inquisitor import create_query_client, execute_query, execute_query_and_return_as_dataframe

import boto3
from botocore.exceptions import ClientError

def configure_client():
    profile_name = 'nocklab'
    region = "us-east-1"

    client = create_query_client(region, profile=profile_name)
    return client

def get_all_ppts(client=None, table_name="eda"):
    """Get a list of all participants in the database.
    Returns: A list of all participants in the database.
    """
    if not client:
        client = configure_client()

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

def drop_low_values(df, ppt_id, output_dir, threshold=0.03):
    """Drop all rows in a dataframe if 90% of the measurements in a 10 second window are below the threshold.
    Parameters: df (pandas dataframe): The dataframe to be filtered.
                threshold (float): The threshold value.
    Returns: A filtered dataframe.
    """
    # grab the first and last time from the df
    starting_len = df.shape[0]
    df.time = pd.to_datetime(df.time)
    first_time = df.time.min()

    # add a column for the number of seconds since the first time
    df['seconds_since_first'] = (df.time - first_time).dt.total_seconds()
    # bin the seconds into 10sec windows -- it is MUCH faster to do it this way then to for loop
    df['10s_from_first'] = df['seconds_since_first'] // 10
    grp = df.groupby('10s_from_first')
    # drop all rows where 90% of the values are below the threshold
    df['drop_group'] = grp.value.transform(lambda x: (x < threshold).sum() / len(x) > 0.9)

    # save the times that will be dropped
    _ = extract_times_that_will_be_dropped(df, ppt_id, output_dir)

    df = df.loc[~df.drop_group].copy()

    ending_len = df.shape[0]

    return df, starting_len, ending_len

def extract_times_that_will_be_dropped(df, ppt_id, output_dir):
    """Extract the times that will be dropped from a dataframe.

    """
    # find continuous blocks of time that will be dropped
    dropped_windows = df.loc[df['drop_group'], '10s_from_first'].unique()
    # find the start and end times of each block
    start_times = []
    end_times = []
    for window in dropped_windows:
        start_times.append(df.loc[df['10s_from_first'] == window, 'time'].min())
        end_times.append(df.loc[df['10s_from_first'] == window, 'time'].max())
    # create a dataframe with the start and end times
    drop_df = pd.DataFrame({'start_time': start_times, 'end_time': end_times})
    ## save the dataframe to a csv
    # make the output directory if it doesn't exist
    if not os.path.exists(f'{output_dir}/dropped_times'):
        os.makedirs(f'{output_dir}/dropped_times')
    # save the dataframe
    drop_df.to_csv(f'{output_dir}/dropped_times/{ppt_id}.csv', index=False)
    return drop_df

def get_wear_time_by_day(df):
    """Return a grouped dataframe with the number of minutes of wear time per day, and the corresponding percent"""
    df['date'] = df.loc[:, 'time'].dt.date
    grp = df.groupby(['date', 'dev_id']).count()  # number of rows per day
    grp['minutes_worn'] = grp.time / 4 / 60  # number of minutes per day
    grp['percent_worn'] = grp.minutes_worn / 60 / 24  # percent of the day
    return grp.reset_index()

def create_wear_time_summary(ppt_list=[], list_filter=None, output_dir='.', save=True, verbose=False):
    """
    """

    client = configure_client()
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
        # breakpoint()
        df, starting_len, ending_len = drop_low_values(df, ppt_id, output_dir, threshold=0.03)
        dropped_pct = round((starting_len - ending_len) / starting_len * 100, 2)
        print(f"Participant {ppt_id} had {starting_len - ending_len} ({dropped_pct}%) rows dropped.")

        # summarize the remaining data by day
        grp = get_wear_time_by_day(df)
        grp['ppt_id'] = ppt_id
        # select only the columns we want
        grp = grp.loc[:, ['ppt_id', 'dev_id', 'date', 'minutes_worn', 'percent_worn']]

        # concat the new data to the existing dataframe
        summary_df = pd.concat([summary_df, grp], ignore_index=True)

    if save:
        summary_df.to_csv(f'{output_dir}/wear_time_summary.csv', index=False)
    return summary_df


def get_duplicate_stats(df, is_acc=False):
    base_subset = ['Time', 'ppt_id', 'dev_id']
    val_subset = ['Time', 'ppt_id', 'dev_id', 'x', 'y', 'z'] if is_acc else ['Time', 'ppt_id', 'dev_id', 'MeasureValue']
    mask_all = df.duplicated(subset=base_subset, keep=False)
    num_dupes_all = mask_all.sum()
    num_rows_all = mask_all.shape[0]
    print("# dupes (all): ", num_dupes_all)
    print("# rows (all): ", num_rows_all, f" ({round(num_dupes_all/num_rows_all, 2)*100}%)")
    num_na_dupes = df[mask_all].x.isna().sum() if is_acc else df[mask_all].MeasureValue.isna().sum()
    print("# dupes (NaN): ", num_na_dupes)
    mask_perf = df.duplicated(subset=val_subset, keep=False)
    num_dupes_perf = mask_perf.sum()
    print("# dupes (perf): ", num_dupes_perf, f" ({round(num_dupes_perf/num_rows_all, 2)*100}%)")
    mask_unc = df.duplicated(subset=base_subset, keep=False) & ~df.duplicated(subset=val_subset, keep=False)
    num_dupes_unc = mask_unc.sum()
    print("# dupes (unc): ", num_dupes_unc, f"({round(num_dupes_unc/num_rows_all,2)*100}%)")

    total_ppts = df.ppt_id.nunique()
    print("# ppts: ", total_ppts)
    print("# ppts with dupes: ", df[mask_all].ppt_id.nunique(), f"({round(df[mask_all].ppt_id.nunique()/total_ppts,2)*100}%)")
    nan_ppts = df.loc[df.x.isna(), 'ppt_id'].nunique() if is_acc else df.loc[df.MeasureValue.isna(), 'ppt_id'].nunique()
    print("# ppts with NaN dupes: ", nan_ppts, f"({round(nan_ppts/total_ppts,2)*100}%)")
    perf_ppts = df[mask_perf].ppt_id.nunique()
    print("# ppts with perf dupes: ", perf_ppts, f"({round(perf_ppts/total_ppts,2)*100}%)" )
    unc_ppts = df[mask_unc].ppt_id.nunique()
    print("# ppts with unc dupes: ", unc_ppts, f"({round(unc_ppts/total_ppts,2)*100}%)" )
    return mask_unc
