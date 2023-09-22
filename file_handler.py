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
    file_paths = sorted(glob.glob(os.path.join(output_dir, "Stage2-deduped_eda_cleaned", month, "*", "*", '*', '*', "*.csv")))

    # deduplicate all and clean the EDA files (in place in Stage2)
    deduplicate_and_clean(file_paths=file_paths, verbose=verbose, output_dir=output_dir)

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
        pattern = r'unzipped/Sensors_[Uu]\d{2}_ALLSITES_|unzipped/Sensors_[Uu]\d{2}_MGH_'
        dest = re.sub(pattern, 'Stage2-deduped_eda_cleaned/', path)
        # create the directories but make sure not to include the filename itself as a dir
        os.makedirs(os.sep.join(dest.split(os.sep)[:-1]), exist_ok=True)
        shutil.copy(path, dest)

def deduplicate_and_clean(file_paths=None, month=None, verbose=False, output_dir='.'):
    # if not file_paths and month:
    #     dir = "Stage2-deduped_eda_cleaned"
    #     file_paths = glob.glob(os.path.join(output_dir, dir, month, "*", "*.csv"))
    for path in tqdm(file_paths, desc="Dropping duplicates", disable=not verbose, leave=True, total=len(file_paths),
                     unit="file"):

        if "acc.csv" in path:
            names = ["time", "x", "y", "z"]
        else:
            names = ["time", "measure_value"]
        df = pd.read_csv(path, names=names, header=0, dtype=str)
        # handle weird -0.0 values in eda
        if "eda" in path.split(os.sep)[-1]:
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
        # get the participant and device ids from the path
        dev_id, ppt_id  = extract_ids_from_path(path)
        # get the month from the path
        month = re.findall(r'\d{8}_\d{8}', path)[0]
        stream = path.split(os.sep)[-1].split(".")[0]

        drop_log = {
            'ppt_id': ppt_id,
            'dev_id': dev_id,
            'month': month,
            'stream': stream,
            'perfect': 0,
            'nan': 0,
            'unclear': 0,
            'total_rows': 0,
            'total_dupes': 0,
        }

        # create the log file if it doesn't exist

        log_path = './logs/duplicate_log.csv' if not 'test' in path else './test_data/duplicate_handling/logs/test_duplicate_log.csv'
        if not os.path.exists(log_path):
            os.makedirs(os.sep.join(log_path.split(os.sep)[:-1]), exist_ok=True) # don't turn the filename into a dir
            drop_df = pd.DataFrame(columns=drop_log.keys())
        else:
            drop_df = pd.read_csv(log_path)
            log_index = (drop_df.ppt_id == ppt_id) & \
                        (drop_df.dev_id == dev_id) & \
                        (drop_df.month == month) & \
                        (drop_df.stream == stream)
            # grab the row for this participant&dev&month&stream if it exists
            if len(drop_df[log_index]) > 0:
                drop_log = drop_df[
                    log_index
                    ].to_dict('records')[0]


        # values of the measure vary by stream
        drop_log['ppt_id'] = ppt_id
        drop_log['dev_id'] = dev_id
        drop_log['month'] = month

        is_acc = 'x' in df.columns  # check if the columns for accelerometer data are present
        total_rows = df.shape[0]
        drop_log['total_rows'] = total_rows
        # count the total number of duplicates
        mask_all = df.duplicated(subset=['time'], keep=False)
        drop_log['total_dupes'] = df[mask_all].shape[0]

        # count the NaNs
        drop_log['nan'] = df.x.isna().sum() if is_acc else df.measure_value.isna().sum()

        # count the perfect duplicates -- entire row is duplicated
        mask_perf = df.duplicated(keep=False)
        drop_log['perfect'] = df[mask_perf].shape[0]

        # count the unclear values -- time is duplicated but other values are different
        mask_unclear = mask_all & ~mask_perf
        drop_log['unclear'] = df[mask_unclear].shape[0]

        # otherwise drop the duplicates
        if not scan_only:
            # drop the rows with unclear values
            df = df[~mask_unclear]

            # drop the rows with NaNs
            df = df.dropna()
            # drop the perfect duplicates (all columns)
            df = df.drop_duplicates(keep='last') # based on recommendation by Giulia via email

        # append the current log to the file
        # currently this allows rescans of the same file to be added to the log multiple times
        # ...not sure if that's a problem, but you could grab by the latest index if needed
        log_index = (drop_df.ppt_id == ppt_id) & \
                    (drop_df.dev_id == dev_id) & \
                    (drop_df.month == month) & \
                    (drop_df.stream == stream)

        if drop_df[log_index].shape[0] > 0:
            # update row if it exists
            for k, v in drop_log.items():
                drop_df.loc[log_index, k] = v
        else:
            # append 'drop_log' as a new analysis round
            drop_df = pd.concat([drop_df, pd.DataFrame([drop_log])], ignore_index=True)

        drop_df.to_csv(log_path, index=False)
        return df

    if file_paths:
        for path in file_paths:
            df = pd.read_csv(path)
            df = drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)
            # replace the old file with the new one without the duplicates
            df.to_csv(path, index=False)
    elif df is not None:
        df = drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)
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
        df_size_bytes = dd.compute(ddf.memory_usage(index=True, deep=True).sum())[0]
        n_partitions = max(df_size_bytes // (1024 ** 3), 1)

        if "test" in output_path:
            # Write the Dask DataFrame to a CSV file without the progress bar
            ddf.repartition(npartitions=n_partitions).to_csv(output_path, index=False)
        else:
            with ProgressBar():
                # Write the Dask DataFrame to a CSV file
                ddf.repartition(npartitions=n_partitions).to_csv(output_path, index=False)





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


def repartition_data(month_path):
    """
    Use this function to repartition the data in Stage3 if the files are too small for some reason
    """
    streams = ['eda', 'acc', 'temp']
    for stream in streams:
        # Define subpath
        subpath = os.path.join(month_path, stream, "*.csv")
        # Create a Dask DataFrame
        ddf = dd.read_csv(subpath)
        with ProgressBar():
            # Compute dataframe size
            df_size_bytes = dd.compute(ddf.memory_usage(index=True, deep=True).sum())[0]
            # Determine number of partitions such that each partition is ~1GB
            n_partitions = max(df_size_bytes // (1024 ** 3), 1)
            output = os.path.join(month_path, f"repartition_{stream}", f"{stream}_combined_*.csv")
            # Repartition dataframe
            ddf.repartition(npartitions=n_partitions).to_csv(output, index=False)


def smell_test(month, ppt_id=None, dev_id=None, stream=None):
    """Grab a row from the log and check that it's been handled correctly

    Check the log file for a row that contains all three types of duplicates (perfect, nan, and unclear) and grab
    the first row that meets this criterion. If no such row is found, grab the first row that contains two types of
    duplicates. The function then checks the original, Stage2, and Stage3 files for the given row to make sure that
    the number of rows match up and that the perfect, nan, and unclear duplicates have been dropped.
    Parameters:
    month (str): The month for which to check the log row.
    Returns:
    None
    Examples:
    smell_test('2021-09') # Check the log row for the month of 2021-09.
    """
    log_path = './logs/duplicate_log.csv'
    log_df = pd.read_csv(log_path)
    log_df = log_df.loc[log_df.month == month]
    ## grab a row with all three types of duplicates if possible, preferably not acc
    log_df = log_df.sort_values(by="stream", ascending=False) # sort by stream so that acc is last
    # grab the first row that has all three types of duplicates
    all_types = log_df.loc[(log_df.perfect > 0) & (log_df.nan > 0) & (log_df.unclear > 0)]
    if all_types.shape[0] == 0:
        print("No rows with all three types of duplicates found. Grabbing a row with two types of duplicates.")
        two_types = all_types.loc[(all_types.unclear > 0) & (all_types.nan > 0)]
        if two_types.shape[0] == 0:
            print("No rows with unclear and nan's found. Do a visual smell test as well.")
            row = log_df.loc[(log_df.unclear > 0) | (log_df.perfect > 0) | (log_df.nan > 0)].iloc[0]
        else:
            row = two_types.iloc[0]
    else:
        row = all_types.iloc[0]
    ppt_id = row.ppt_id if ppt_id is None else ppt_id
    dev_id = row.dev_id if dev_id is None else dev_id
    stream = row.stream if stream is None else stream
    row = log_df.loc[(log_df.ppt_id == ppt_id) & (log_df.dev_id == dev_id) & (log_df.stream == stream)].iloc[0]

    # exit the test if there were no duplicates to begin with
    if row.total_dupes == 0:
        print("No duplicates to test. Exiting.")
        return

    # some files are too big to read into memory so we have to handle these separately
    if row.total_rows > 75000000 and row.total_dupes > 0:
        print(f"File for {month, stream, ppt_id, dev_id} too large to read into memory. Skipping smell test.")
        return

    print(f"Checking {ppt_id} {dev_id} {stream} in {month}")
    og_path = glob.glob(f"./data/unzipped/*_{month}/*/*/{ppt_id[-3:]}/{dev_id}/{stream}.csv")[0]
    df_original = pd.read_csv(og_path, dtype=str)

    stage2_paths = glob.glob(f"./data/Stage2-deduped_eda_cleaned/{month}/*/*/{ppt_id[-3:]}/{dev_id}/{stream}.csv")
    df_stage2 = pd.concat([pd.read_csv(path, dtype=str) for path in stage2_paths])

    # have to find the stage3 path by looking for the ppt_id in the file
    stage3_paths = glob.glob(f"./data/Stage3-combined_and_ready_for_upload/{month}/{stream}/{stream}_combined_*.csv")
    stage3_paths_with_ppt = []
    for path in tqdm(stage3_paths, desc="Reading paths looking for ppt", leave=False, unit="file", position=0, total=len(stage3_paths), ncols=100):
        # we need to make sure we get all rows for the ppt_id
        with open(path, 'r') as file:
            content = file.read()
        if ppt_id in content:
            stage3_paths_with_ppt.append(path)
    df_stage3 = pd.concat([pd.read_csv(path, dtype=str) for path in stage3_paths_with_ppt])
    df_stage3 = df_stage3.loc[(df_stage3.ppt_id == ppt_id) & (df_stage3.dev_id == dev_id)]


    # check that the number of rows in Stage2 and Stage3 are the same
    assert df_stage2.shape[0] == df_stage3.shape[0], f"Stage2 ({df_stage2.shape[0]}) and Stage3 ({df_stage3.shape[0]}) have different numbers of rows"
    # check that the total rows in the log and the total rows in the original are the same
    assert df_original.shape[0] == row.total_rows, "Original rows != total rows"

    # use column index in calls rather than typing out the different names with spaces pre-cleaning
    # idx 0 = time, idx 1 is measure value
    time_col = df_original.columns[0]
    measure_col = df_original.columns[1]
    perf_mask = df_original.duplicated(keep=False)
    # check that an example of perfect, nan, and unclear is not in Stage2
    perfect_rows = df_original.loc[perf_mask]
    perfect = perfect_rows.iloc[0] if perfect_rows.shape[0] > 0 else None
    if perfect is not None:
        # assert only one row with the perfect time in Stage2 and Stage3
        assert df_stage2.loc[df_stage2.time == perfect[time_col]].shape[0] == 1
        assert df_stage3.loc[df_stage3.time == perfect[time_col]].shape[0] == 1
        assert df_stage2.duplicated(keep=False).sum() == 0
        assert df_stage3.duplicated(keep=False).sum() == 0
    del perfect_rows
    del perfect

    had_na = df_original.isna().sum().sum() > 0
    if had_na:
        # assert nan row has been dropped
        assert df_stage2.isna().sum().sum() == 0
        assert df_stage3.isna().sum().sum() == 0

    unclear_rows = df_original.loc[df_original.duplicated(time_col, keep=False) & ~perf_mask]
    unclear = unclear_rows.iloc[0] if unclear_rows.shape[0] > 0 else None
    if unclear is not None:
        # assert unclear row has been dropped
        assert df_stage2.loc[df_stage2.time == unclear[time_col]].shape[0] == 0
        assert df_stage3.loc[df_stage3.time == unclear[time_col]].shape[0] == 0
        assert df_stage2.duplicated(['time'], keep=False).sum() == 0
        assert df_stage3.duplicated(['time', 'dev_id'], keep=False).sum() == 0
    del unclear_rows
    del unclear
    print("Smell test passed!")




