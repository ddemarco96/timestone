"""A set of utilities for handling zipped files and directories"""
import csv
import os
import zipfile
import shutil
import re
import pandas as pd
import glob
from tqdm import tqdm
from dotenv import load_dotenv
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import requests
import json
import logging
load_dotenv()

log = logging.getLogger(__name__)

def unzip_walk(file_path, cleanup=True):
    """Unzip a file and return a list of file paths to any eda, temp, or acc csvs files within the unzipped directory.
    Parameters:
        file_path (str): The path to the file to be unzipped.
        cleanup (bool, optional): Whether to remove the unzipped directory after the function has finished.
            Defaults to True.
    Returns:
        # NB the unzipped dir is in the same dir as the file_path
        list: A list of file paths to any eda, temp, or acc csvs files within the unzipped directory.
    Examples:
        file_paths = unzip_walk('/path/to/file.zip')
        # file_paths ['/path/to/unzipped/file1.csv', '/path/to/unzipped/file2.csv', ...]
    """
    # 1. Find the parent dir of the file_path
    grandparent_dir = os.path.dirname(os.path.dirname(file_path))
    unzipped_dir = os.path.join(grandparent_dir, "unzipped")
    os.makedirs(unzipped_dir, exist_ok=True)
    # 2/3. Unzip the file and move all to unzipped_dir
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_name = zip_ref.filename.split(os.sep)[-1][0:-4]
        target_path = os.path.join(unzipped_dir, zip_name)
        # this can get messed up depending on whether foo.zip creates a dir foo or not
        zip_ref.extractall(target_path)
    # 4. return the list of file paths to any eda, temp, or acc csvs files in any dir within the unzipped dir
    file_paths = []
    for root, dirs, files in os.walk(target_path):
        for file in files:
            if file.endswith(".csv") and ("eda" in file or "temp" in file or "acc" in file):
                file_paths.append(os.path.join(root, file))
    # cleanup by removing the unzipped dir if you want
    if cleanup:
        shutil.rmtree(unzipped_dir)
    return file_paths

def simple_walk(dir_path):
    """Walk through a directory and return paths to all .csv files containing "eda", "temp" or "acc" in their names.
    Parameters: dir_path (str): The path to the directory to walk through.
    Returns: list: A list of strings containing paths to the relevant .csv files
    Examples:
       simple_walk('/home/user/data')
       # ['/home/user/data/eda.csv', '/home/user/data/temp.csv', '/home/user/data/acc.csv']
    """
    file_paths = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file.endswith(".csv") and ("eda" in file or "temp" in file or "acc" in file):
                file_paths.append(os.path.join(root, file))

    return file_paths

def extract_ids_from_path(file_path):
    """
    example path:
        'Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv'
    1. Split the file_path on /
    2. Get the device_id from the last index before the files
    3. Get the ppt_id from the previous two levels
    """
    path_list = file_path.split(os.sep)
    device_id = path_list[-2]
    ppt_id = path_list[-4].lower() + path_list[-3]
    return device_id, ppt_id

def extract_streams_from_pathlist(file_paths, streams):
    """Extract the desired streams from a list of file paths.

    Given a list of file paths and a comma-separated list of streams, this function will filter the list of paths
    to only include those which contain one of the desired streams.

    Parameters:
        file_paths (list): A list of file paths
        streams (str): A comma-separated list of streams

    Returns:
        list: A filtered list of file paths containing one of the desired streams

    Examples:
        streams = 'mp4,wav'
        file_paths = ['/path/to/file1.avi', '/path/to/file2.wav', '/path/to/file3.mp4']
        filtered_paths = extract_streams_from_pathlist(file_paths, streams)
        # filtered_paths = ['/path/to/file2.wav', '/path/to/file3.mp4']
    """
    stream_list = streams.split(",")
    # remove any paths which do not contain one of the streams we want
    filtered_paths = []
    for file_path in file_paths:
        if any(stream in file_path for stream in stream_list):
            filtered_paths.append(file_path)
    return filtered_paths

def create_output_file(output_path: str, stream: str) -> None:
    """
    Creates a new output file with headers based on the stream type
    """
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if stream == "acc":
            writer.writerow(['time', 'x', 'y', 'z', 'dev_id', 'ppt_id'])
        else:
            writer.writerow(['time', 'measure_value', 'dev_id', 'ppt_id'])

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html


def raw_to_batch_format(file_paths, output_dir='.', verbose=False, streams='eda,temp,acc'):
    """
    Stage 1 - raw/unzipped files
    Stage 2 - original structure but deduplicated and -0 values replaced with 0
    Stage 3 - combined files with ppt_id and dev_id columns
    """
    is_test = False # use different directories and block slack notifications
    if 'test' in file_paths[0]:
        output_dir = './test_data'
        is_test = True
    file_paths = sorted(file_paths)
    # extract the month from the first file path
    month = re.findall(r'\d{8}_\d{8}', file_paths[0])[0]
    # create the stage directories is they don't exist
    stage_2_path = os.path.join(output_dir, 'Stage2-deduped_eda_cleaned')
    stage_3_path = os.path.join(output_dir, 'Stage3-combined_and_ready_for_upload')
    os.makedirs(stage_2_path, exist_ok=True)
    os.makedirs(stage_3_path, exist_ok=True)



    # copy the unzipped dir to Stage2 where they will be deduplicated and cleaned
    copy_files_to_stage2(file_paths, output_dir=stage_2_path, verbose=verbose)

    # get the paths to the copied files
    file_paths = glob.glob(os.path.join(output_dir, "Stage2-deduped_eda_cleaned", month, "*", "*", '*', '*', "*.csv"))
    # deduplicate all and clean the EDA files (in place in Stage2)
    deduplicate_and_clean(month=month, verbose=verbose, output_dir=output_dir)

    # take the files that are split by device and ppt and combine them into one file per stream adding ppt_id and dev_id
    # save the output to Stage3 for upload
    combine_files_and_add_columns(month=month, output_dir=output_dir, verbose=verbose, streams=streams)
    send_slack_notification("Columns added, duplicates dropped", test=is_test)

def copy_files_to_stage2(file_paths, output_dir='.', verbose=False):
    for path in tqdm(
            file_paths,
            desc="Copying files to Stage2",
            disable=not verbose,
            leave=True,
            total=len(file_paths),
            unit="file"):


        # remove everything except the month from the top level
        pattern = r'unzipped/Sensors_[Uu]\d{2}_ALLSITES_'
        dest = re.sub(pattern, 'Stage2-deduped_eda_cleaned/', path)
        # create the directories but make sure not to include the filename itself as a dir
        os.makedirs(os.sep.join(dest.split(os.sep)[:-1]), exist_ok=True)
        shutil.copy(path, dest)

def deduplicate_and_clean(file_paths=None, month=None, verbose=False, output_dir='.'):
    if not file_paths and month:
        dir = "Stage2-deduped_eda_cleaned"
        file_paths = glob.glob(os.path.join(output_dir, dir, month, "*", "*.csv"))
    for path in tqdm(file_paths, desc="Dropping duplicates", disable=not verbose, leave=True, total=len(file_paths),
                     unit="file"):

        if "acc.csv" in path:
            names = ["time", "x", "y", "z"]
        else:
            names = ["time", "measure_value"]
        df = pd.read_csv(path, names=names, header=0, dtype=str)
        # handle weird -0.0 values in eda
        if "eda" in path:
            # convert any measures of "-0.0" to "0.0"
            df['measure_value'] = df['measure_value'].replace("-0.0", "0.0")

        # handle duplicates
        handle_duplicates(df=df, scan_only=False, path=path, verbose=verbose)

def handle_duplicates(file_paths=None, df=None, path=None, scan_only=True, verbose=False):
    """Removes and logs participants with duplicate data.

    Args:
        file_paths (list of str): A list of file paths to process.

    Returns:
        found_duplicates (df): df with columns "path" and "duplicates" for which participants were removed
    """
    def drop_from_df(df, scan_only, path=None, verbose=False):
        """Drops duplicates from a dataframe and logs them to a csv file."""
        drop_log = {
            'path': path,
            'perfect': 0,
            'nan': 0,
            'unclear': 0,
            'total_rows': 0,
            'total_dupes': 0,
            'total_ppts': ',',
            'dupe_ppts': ',',
        }
        # if a drop log exists and this isn't a test, use that instead
        if os.path.exists('./logs/duplicate_log.csv') and path and not 'test' in path:
            drop_df = pd.read_csv('./logs/duplicate_log.csv')
            if path in drop_df.path.values:
                drop_log = drop_df[drop_df.path == path].to_dict('records')[0]
        log_output_path = './logs/duplicate_log.csv' if not 'test' in path else './test_data/duplicate_handling/logs/test_duplicate_log.csv'
        is_acc = 'x' in df.columns  # check if the columns for accelerometer data are present
        # values of the measure vary by stream
        total_ppts = df.ppt_id.unique()
        total_ppts_str = ','.join([ppt for ppt in total_ppts if ppt not in drop_log['total_ppts']])
        drop_log['total_ppts'] = drop_log['total_ppts'] + total_ppts_str

        total_rows = df.shape[0]
        drop_log['total_rows'] += total_rows
        # count the total number of duplicates
        mask_all = df.duplicated(subset=['time', 'ppt_id', 'dev_id'])
        drop_log['total_dupes'] += df[mask_all].shape[0]
        dupe_ppts = df[mask_all].ppt_id.unique()

        if len(dupe_ppts) > 0:
            try:
                dupe_ppts_str = ','.join([ppt for ppt in dupe_ppts if ppt not in drop_log['dupe_ppts']])
                drop_log['dupe_ppts'] = drop_log['dupe_ppts'] + dupe_ppts_str
            except:
                breakpoint()


        mask_perf = df.duplicated()
        # count the NaNs
        drop_log['nan'] += df.x.isna().sum() if is_acc else df.measure_value.isna().sum()
        # count the perfect duplicates
        drop_log['perfect'] += mask_perf.sum()
        # count the unclear values
        drop_log['unclear'] += df.duplicated(subset=['time', 'ppt_id', 'dev_id']).sum() - mask_perf.sum()

        # otherwise drop the duplicates
        if not scan_only:
            # drop the rows with NaNs
            df.dropna(inplace=True)
            # drop the perfect duplicates (all columns)
            df.drop_duplicates(inplace=True)
            # drop the rows with unclear values
            df.drop_duplicates(subset=['time', 'ppt_id', 'dev_id'], inplace=True)
            # check if the file exists yet
            if not os.path.exists(log_output_path):
                # create the file and add the header
                pd.DataFrame(columns=[
                    'path',
                    'perfect',
                    'nan',
                    'unclear',
                    'total_rows',
                    'total_dupes',
                    'total_ppts',
                    'dupe_ppts',
                ]).to_csv(log_output_path, index=False)
        # append the current log to the file
        pd.DataFrame([drop_log]).to_csv(log_output_path, index=False, mode="a", header=False)

    if file_paths:
        for path in file_paths:
            df = pd.read_csv(path)
            drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)
            # replace the old file with the new one without the duplicates
            df.to_csv(path, index=False)
    elif df is not None:
        drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)
        df.to_csv(path, index=False)

def combine_files_and_add_columns(file_paths=None, month=None, output_dir='.', verbose=False, streams='eda,temp,acc'):
    """Processes files of a given stream type from a list of paths, and writes them to output files in the given output directory.
        Parameters:
            •	file_paths (list): List of file paths to process
            •	streams (str): Comma-separated string of stream types to process
            •	output_dir (str): Output directory to write files to
            •	verbose (bool): Whether to print progress messages
        Returns:
            •	month (str): Month of the files being processed
        Examples:
        month = process_files(file_paths, streams="acc,gyro", output_dir="/data/output/", verbose=True)
    """
    # example path
    # Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv
    # you can optionally extract the month name from the supplied filepaths but by default just use the month arg

    month = re.findall(r'\d{8}_\d{8}', file_paths[0])[0] if file_paths else month
    if not file_paths:
        file_paths = glob.glob(os.path.join(output_dir, "Stage2-deduped_eda_cleaned", month, "*", "*", '*', '*', "*.csv"))

    for stream in tqdm(streams.split(","),
                            disable=(not verbose),
                            desc="Processing streams",
                            unit="streams",
                            ncols=100,
                            position=0,
                            leave=True,
                            total=len(streams.split(","))
                            ):

        stream_paths = [path for path in file_paths if stream in path.split(os.sep)[-1]]
        if len(stream_paths) == 0:
            continue
            # Initialize an empty Dask DataFrame
        ddf = dd.from_pandas(pd.DataFrame([], dtype="object"), npartitions=1)
        for path in stream_paths:
            device_id, ppt_id = extract_ids_from_path(path)
            stream_in_name = path.split(os.sep)[-1].split(".")[0]
            assert stream == stream_in_name

            # Read the file into a Dask DataFrame
            chunk_ddf = dd.read_csv(path, assume_missing=False, dtype=str)

            # Add the ppt_id and dev_id columns
            chunk_ddf = chunk_ddf.assign(ppt_id=ppt_id)
            chunk_ddf = chunk_ddf.assign(dev_id=device_id)
            # Append the chunk DataFrame to the main DataFrame
            ddf = dd.concat([ddf, chunk_ddf], interleave_partitions=True)

        stream_dir = os.path.join(output_dir, "Stage3-combined_and_ready_for_upload", month, stream)
        os.makedirs(stream_dir, exist_ok=True)
        output_path = os.path.join(stream_dir, f"{stream}_combined_*.csv")
        if "test" in output_path:
            # Write the Dask DataFrame to a CSV file without the progress bar
            ddf.repartition(partition_size="900MB").to_csv(output_path, index=False)
        else:
            with ProgressBar():
                # Write the Dask DataFrame to a CSV file
                ddf.repartition(partition_size="900MB").to_csv(output_path, index=False)





def send_slack_notification(message=None, test=False):
    """
    Sends a message to a Slack channel
    Parameters:
    webhook_url (str): Slack webhook url
    message (str): Content of the message
    """
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if message is None:
        message = "A function has completed running."
    data = {'text': message}
    log.info(f"{message}")
    if not test:
        response = requests.post(webhook_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
        if response.status_code != 200:
            raise ValueError(f'Request to slack returned an error {response.status_code}, the response is:\n{response.text}')


