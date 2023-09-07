"""A set of utilities for handling zipped files and directories"""
import csv
import os
import sys
import time
import zipfile
import shutil
import re
import pandas as pd
import threading
import math
import glob
from tqdm import tqdm

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
    if not os.path.exists(unzipped_dir):
        os.mkdir(unzipped_dir)
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
    if 'test' in file_paths[0]:
        output_dir = './test_data'
        os.makedirs(os.path.join(output_dir, 'prepped'), exist_ok=True)
    file_paths = sorted(file_paths)
    # take the files split by device and ppt and combine them into one file per stream adding ppt_id and dev_id
    month = combine_files_and_add_columns(file_paths, output_dir, verbose, streams)
    # handle duplicates and move to cleaned_and_combined
    process_combined_files(month=month, verbose=verbose, output_dir=output_dir)

    processed_paths = glob.glob(os.path.join(output_dir, "prepped", month, "*", "*.csv"))
    # recombine cleaned files to minimize number of files needed to be uploaded
    recombine_cleaned_files(file_paths=processed_paths, output_dir=os.path.join(output_dir, 'cleaned_and_combined', month))

def combine_files_and_add_columns(file_paths, output_dir='.', verbose=False, streams='eda,temp,acc'):
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

    start_time = time.time()
    output_paths = []
    month = re.findall(r'\d{8}_\d{8}', file_paths[0])[0] if file_paths else None
    for stream_type in streams.split(","):
        output_index = 0
        for path in tqdm(file_paths,
                         disable=(not verbose),
                         desc=f"Processing {stream_type} files",
                         unit="files",
                         ncols=100,
                         position=0,
                         leave=True,
                         total=len([name for name in file_paths if stream_type in name])
                         ):
            month = re.findall(r'\d{8}_\d{8}', path)[0]
            device_id, ppt_id = extract_ids_from_path(path)
            stream = path.split(os.sep)[-1].split(".")[0]
            if stream != stream_type:
                continue

            chunks_read = 0
            records_read = 0
            current_size = 0
            optimized_sizes = {
                "acc": 21474836,
                "eda": 23860929,
                "temp": 23860929,
            }
            chunksize = optimized_sizes[stream]  # pre-calculate using the get_optimal_chunksize function
            if "acc.csv" in path:
                names = ["time", "x", "y", "z"]
                dtypes = {"time": "str", "x": "str", "y": "str", "z": "str"}
            else:
                names = ["time", "measure_value"]
                dtypes = {"time": "str", "measure_value": "str"}

            with open(path, 'rb') as f:
                number_of_rows = sum(1 for _ in f)
                chunk_count = math.ceil(number_of_rows / chunksize)

            with pd.read_csv(path, header=0, names=names, chunksize=chunksize, dtype=dtypes, parse_dates=['time']) as reader:
                for chunk in tqdm(reader,
                                  leave=False,
                                  desc=f"Processing chunks...",
                                  disable=(not verbose),
                                  total=chunk_count,
                                  ):  # each chunk is a df
                    chunks_read += 1
                    records_read += chunk.shape[0]
                    chunk['dev_id'] = device_id
                    chunk['ppt_id'] = ppt_id


                    # check if an output file exists in the path `prepped/[month]/[stream]/combined_[index].csv`
                    output_path = os.path.join(output_dir, "prepped", month, stream, f"{stream}_combined_{output_index}.csv")
                    if not os.path.exists(os.path.dirname(output_path)):
                        os.makedirs(os.path.dirname(output_path))
                        if not os.path.exists(output_path):
                            # print(f"Creating new file: {output_path}") if verbose else None
                            create_output_file(output_path, stream)
                            current_size = 0

                    # if it does, check its size
                    output_size = current_size + chunk.memory_usage(deep=True).sum()
                    # if the size of the file would be > 4.9GB (AWS max is 5GB) after adding the dataframe to it,
                    if output_size > 4.9 * 1e9:
                        # increment index and create a new file
                        output_index += 1
                        output_path = os.path.join(output_dir, "prepped", month, stream, f"{stream}_combined_{output_index}.csv")
                        # print(f"Creating new file: {output_path}") if verbose else None
                        create_output_file(output_path, stream)
                        current_size = 0

                    # append the contents of the dataframe to the output target csv
                    # include the header only if it's a new file
                    chunk.to_csv(output_path, mode="a", header=False, index=False)
                    current_size += chunk.memory_usage(deep=True).sum()
                    output_paths.append(output_path) if output_path not in output_paths else None
    print(f"Processed {records_read} records in {time.time() - start_time} seconds.") if verbose else None
    return month

def process_combined_files(file_paths=None, month=None, verbose=False, final_pass=False, output_dir='.'):
    if not file_paths and month:
        dir = "prepped" if not final_pass else "cleaned_and_combined"
        file_paths = glob.glob(os.path.join(output_dir, dir, month, "*", "*.csv"))
    for name in tqdm(file_paths, desc="Dropping duplicates", disable=not verbose, leave=False, total=len(file_paths), unit="file"):
        df = pd.read_csv(name)
        # handle weird -0.0 values in eda
        if "eda" in name:
            # convert any measures of "-0.0" to "0.0"
            df['measure_value'] = df['measure_value'].replace("-0.0", "0.0")

        # handle duplicates
        handle_duplicates(df=df, scan_only=False, path=name, verbose=verbose)


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
            drop_df = pd.read_csv('data/saved_for_comp/duplicate_log.csv')
            if path in drop_df.path.values:
                drop_log = drop_df[drop_df.path == path].to_dict('records')[0]
        output_path = './logs/duplicate_log.csv' if not 'test' in path else './logs/test_duplicate_log.csv'
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
            if not os.path.exists(output_path):
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
                ]).to_csv(output_path, index=False)
        # append the current log to the file
        pd.DataFrame([drop_log]).to_csv(output_path, index=False, mode="a", header=False)

    if file_paths:
        for path in file_paths:
            df = pd.read_csv(path)
            drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)
    elif df is not None:
        drop_from_df(df=df, scan_only=scan_only, path=path, verbose=verbose)

    # replace the old file with the new one without the duplicates
    df.to_csv(path, index=False)

def recombine_cleaned_files(file_paths, max_size=5e9, output_dir='./cleaned_and_combined'):
    """Combine multiple files with duplicates dropped into the minimum number of files of size <5GB.
        Parameters:
        file_paths (list): A list of file paths to be recombined.
        max_size (float): The maximum size of each combined file. Default is 5e9.
        Returns:
        combined_files (list): A list of the combined files.
        Examples:
        combined_files = recombine_cleaned_files(['acc_data.csv', 'eda_data.csv', 'temp_data.csv'], max_size=5e9)
        # combined_files will be a list of two files, each with a size of 5GB or less.
    """
    for stream in ['acc', 'eda', 'temp']:
        try:
            # Create the output directory if it doesn't exist
            os.makedirs(os.path.join(output_dir, stream), exist_ok=True)
            current_file = None
            combined_file_idx = 0
            # filter the paths to the relevant stream
            filtered_paths = sorted([path for path in file_paths if stream in path])
            # we don't need to combine if there's only one file or none
            if len(filtered_paths) == 1:
                destination_path = os.path.join(output_dir, stream, filtered_paths[0].split(os.sep)[-1])
                shutil.copy(filtered_paths[0], destination_path)
                continue
            elif len(filtered_paths) == 0:
                continue

            # estimated row size in bytes (See CSV ingestor for more details)
            sizes = {
                'acc': 47, # time (8) + ppt_id (5) + dev_id (10) + x (8) + y (8) + z (8) = 47
                'eda': 31, # time (8) + ppt_id (5) + dev_id (10) + measure_value (8) = 31
                'temp': 31 # time (8) + ppt_id (5) + dev_id (10) + measure_value (8) = 31
            }
            row_size = sizes[stream]
            # Calculating the maximum number of rows that fit within 100MB, so that we can read in chunks of that size
            # if this is in tests, set smaller sizing for chunks
            rows_per_100MB = 1e8 // row_size if not "test" in filtered_paths[0] else row_size
            num_processed = 0
            current_size = 0
            # Process each file in the current data stream
            while num_processed < len(filtered_paths):
                if current_file is None:
                    # Create the first file
                    current_path = os.path.join(output_dir, stream, f"{stream}_combined_{combined_file_idx}.csv")
                    current_file = open(current_path, "w")
                    current_size = 0
                    # Add the header based on the data stream
                    if "acc" in stream:
                        current_file.write("time,x,y,z,dev_id,ppt_id\n")
                    else:
                        current_file.write("time,measure_value,dev_id,ppt_id\n")

                with pd.read_csv(filtered_paths[num_processed], chunksize=rows_per_100MB) as reader:
                    for chunk in reader:

                        # print("Processing chunk...with size:", chunk.shape[0])
                        # print("Current file size:", current_size, " out of ", max_size)


                        # If adding the current chunk exceeds the maximum size, close the current combined file and create a new one
                        if current_size + chunk.memory_usage(deep=True).sum() > max_size:
                            # switch to a new file and continue
                            current_file.close()
                            combined_file_idx += 1
                            current_path = os.path.join(output_dir, stream, f"{stream}_combined_{combined_file_idx}.csv")
                            current_file = open(current_path, "w")
                            current_size = 0
                            # Add the header based on the data stream
                            if "acc" in stream:
                                current_file.write("time,x,y,z,dev_id,ppt_id\n")
                            else:
                                current_file.write("time,measure_value,dev_id,ppt_id\n")
                        # print("Writing chunk to file...")
                        # Append the current chunk to the current combined file
                        chunk.to_csv(current_file, mode="a", index=False, header=False)
                        current_size += chunk.memory_usage(deep=True).sum()
                # we finished processing the csv, so increment the counter to the next path
                num_processed += 1
        finally:
            if current_file is not None:
                current_file.close()




def optimum_chunk_size(stream):
    """Calculates the optimum chunk size for a given stream.
        Parameters:
        stream (str): The stream to calculate the optimum chunk size for.
        Returns:
        chunk_size (int): The optimum chunk size for the given stream.

    """
    # example of each row
    examples = {
        'pre_acc': "1557330001111,0.562753,-0.476326,0.610117",
        'pre_eda': "1557329999142,45.705120",
        'pre_temp': "1557329998879,23.021000",
        'post_acc': "1557330001111,0.562753,-0.476326,0.610117,2KNY3111P2,mgh001",
        'post_eda': "1557329999142,45.70512,2KNY3111P2,mgh001",
        'post_temp': "1557329998879,23.021,2KNY3111P2,mgh001",
    }
    df = pd.read_csv(StringIO(examples[stream]), header=None)
    mem_usage = df.memory_usage(deep=True).sum() / 1024 ** 2
    print(f"One row uses {mem_usage} MB")
    # 16GB of system memory with est 90% reserved for other processes
    system_memory_total = 16 * 1024
    memory_allocated = 0.1
    rows_in_memory = system_memory_total * memory_allocated / mem_usage
    chunk_size = int(rows_in_memory)
    return chunk_size

def send_slack_notification(message=None):
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
    response = requests.post(webhook_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise ValueError(f'Request to slack returned an error {response.status_code}, the response is:\n{response.text}')


