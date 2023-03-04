"""
The CLI for triggering/handling timestream uploads.

For now we expect data to be nested in the format:
    /Month/participant_id/device_id/[csvs]

"""


import shutil
import argparse

from csv_ingestor import CSVIngestor
import boto3
from botocore.config import Config
import os
import zipfile


def unzip_walk(file_path, cleanup=True):
    """
    1. Find the parent dir of the file_path
    2. Unzip the file
    3. Move the contents to parent_dir/unzipped
    4. return the list of file paths to any eda, temp, or acc csvs files in any dir within the unzipped dir
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

def walking_cost(path_rows, ingestor, verbose):
    """
    1. For each file path, get the file size
    2. Return the sum of all file sizes
    """
    total_cost = 0
    for (path, num_rows) in path_rows:
        total_cost += ingestor.estimate_csv_write_cost(file_path=path, df_rows=num_rows, verbose=verbose)
    return total_cost

def walking_time(path_rows, ingestor, verbose):
    """Return the time in minutes it will take to upload all the files in path_rows"""
    total_time = 0
    for (path, num_rows) in path_rows:
        total_time += ingestor.estimate_csv_write_time(file_path=path, df_rows=num_rows, verbose=verbose)
    return total_time

def extract_ids_from_path(file_path):
    """
    1. Split the file_path on /
    2. Get the device_id from the last index before the files
    3. Get the ppt_id from the previous two levels
    """
    path_list = file_path.split(os.sep)
    device_id = path_list[-2]
    ppt_id = path_list[-4].lower() + path_list[-3]
    return device_id, ppt_id

def extract_streams_from_pathlist(file_paths, streams):
    """
    1. For each file_path, extract the device_id and ppt_id
    2. If the device_id is in the streams list, add it to the list of streams
    3. Return the list of streams
    """
    stream_list = streams.split(",")
    # remove any paths which do not contain one of the streams we want
    filtered_paths = []
    for file_path in file_paths:
        if any(stream in file_path for stream in stream_list):
            filtered_paths.append(file_path)
    return filtered_paths


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file_path", help="This file will be used for ingesting records.")
    parser.add_argument("-s", "--streams", help="Comma separated list of streams to ingest e.g., 'acc,temp'.")
    parser.add_argument("-as", "--all-streams", help="Ingest all streams, ignore -s/--streams.",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("-k", "--kmsId", help="This will be used for updating the database.")
    parser.add_argument("-nw", "--no-warnings", help="Don't Print the cost/time of uploading the file/s in file_path.",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("-fs", "--full_send", help="Write the data to Timestream.",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("-v", "--verbose", help="Call functions with the verbose arg",
                        action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    session = boto3.Session(profile_name='nocklab')

    # Recommended Timestream write client SDK configuration:
    #  - Set SDK retry count to 10.
    #  - Use SDK DEFAULT_BACKOFF_STRATEGY
    #  - Set RequestTimeout to 20 seconds .
    #  - Set max connections to 5000 or higher.
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))

    ingestor = CSVIngestor(write_client)

    # if args.kmsId is not None:
        # csv_ingestion_example.update_database(args.kmsId)

    # check if the file_path is a zip file
    if args.file_path.endswith(".zip"):
        # if it is, unzip it and get the file paths to all the csvs
        file_paths = unzip_walk(args.file_path, cleanup=False)
    else:
        # if it's not, just get the file path
        file_paths = [args.file_path]

    # filter the path list based on --as or -s
    if not args.all_streams:
        if args.streams is None:
            raise ValueError("You must specify a stream to ingest or all streams.")
        else:
            file_paths = extract_streams_from_pathlist(file_paths, args.streams)


    # for each path in file_paths, find the number of rows in that df and save it to a tuple
    path_rows = []
    for file_path in file_paths:
        tup = file_path, ingestor.get_num_rows(file_path)
        path_rows.append(tup)
    # if the user has not specified no-cost, estimate the cost of uploading the file/s
    if not args.no_warnings:
        total_cost = walking_cost(path_rows, ingestor, args.verbose)
        total_time = walking_time(path_rows, ingestor, args.verbose)
        print(f"Total cost of this upload: {total_cost}")
        print(f"Total time required for this upload: {total_time}")
        # confirm with the user that they want to continue
        confirm = input("Continue? (y/n): ")

        if confirm.lower() != "y":
            exit()
    # Actually write the data to Timestream
    if args.full_send:
        confirm_2 = input("Last chance. Are you sure you want to continue? (y/n): ")
        if confirm_2.lower() == "y":
            for (path, num_rows) in path_rows:
                device_id, ppt_id = extract_ids_from_path(path)
                ingestor.write_records_with_common_attributes(file_path=path,
                                                              device_id=device_id,
                                                              participant_id=ppt_id,
                                                              verbose=args.verbose)



