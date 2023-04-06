"""A set of utilities for handling zipped files and directories"""
import csv
import os
import sys
import time
import zipfile
import shutil
import re
import pandas as pd

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
            writer.writerow(['Time', 'x', 'y', 'z', 'ppt_id', 'dev_id'])
        else:
            writer.writerow(['Time', 'MeasureValue', 'ppt_id', 'dev_id'])

def raw_to_batch_format(file_paths, output_dir='.', verbose=False, streams='eda,temp,acc'):
    """Processes a list of file paths and formats them for upload to AWS Timestream in batches.

    Args:
        file_paths (list of str): A list of file paths to process.
        output_dir (str, optional): The directory to output processed files to. Defaults to the current directory.
        verbose (bool, optional): Whether to print out the status of the processing. Defaults to False.
        streams (str, optional): A comma-separated list of streams to process. Defaults to "eda,temp,acc".

    Returns:
        None

    Examples:
        raw_to_batch_upload(['/path/to/file1.csv', '/path/to/file2.csv'], output_dir='/path/to/output', verbose=True, streams='eda,temp')
    """
    # example path
    # Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv
    for stream_type in streams.split(","):
        output_index = 0
        for path in file_paths:
            month = re.findall(r'\d{8}_\d{8}', path)[0]
            ppt_id, device_id = extract_ids_from_path(path)
            stream = path.split(os.sep)[-1].split(".")[0]
            if stream != stream_type:
                continue

            chunks_read = 0
            records_read = 0
            chunksize = 1000000  # 1M rows
            if "acc.csv" in path:
                names = ["Time", "x", "y", "z"]
                dtypes = {"Time": "str", "x": "str", "y": "str", "z": "str"}
            else:
                names = ["Time", "MeasureValue"]
                dtypes = {"Time": "str", "MeasureValue": "str"}

            with pd.read_csv(path, header=0, names=names, chunksize=chunksize, dtype=dtypes) as reader:
                start_time = time.time()

                for chunk in reader:  # each chunk is a df
                    chunks_read += 1
                    records_read += chunk.shape[0]
                    print(f"Processing chunk {chunks_read} with {chunk.shape[0]} records...") if verbose else None
                    chunk['dev_id'] = device_id
                    chunk['ppt_id'] = ppt_id
                    # check if an output file exists in the path `pending_upload/[month]/[stream]/combined_[index].csv`
                    output_path = os.path.join(output_dir, "pending_upload", month, stream, f"combined_{output_index}.csv")
                    if not os.path.exists(os.path.dirname(output_path)):
                        os.makedirs(os.path.dirname(output_path))
                        if not os.path.exists(output_path):
                            print(f"Creating new file: {output_path}") if verbose else None
                            create_output_file(output_path, stream)



                    # if it does, check its size
                    output_size = os.path.getsize(output_path) + sys.getsizeof(chunk)
                    # if the size of the file would be > 4.9GB (AWS max is 5GB) after adding the dataframe to it,
                    if output_size > 4.9 * 10**9:
                        # increment index and create a new file
                        output_index += 1
                        output_path = os.path.join(output_dir, "pending_upload", month, stream, f"combined_{output_index}.csv")
                        print(f"Creating new file: {output_path}") if verbose else None
                        create_output_file(output_path, stream)

                    # append the contents of the dataframe to the output target csv
                    # include the header only if it's a new file
                    chunk.to_csv(output_path, mode="a", header=False, index=False)