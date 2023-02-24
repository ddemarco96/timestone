"""
The CLI for triggering/handling timestream uploads.

For now we expect data to be nested in the format:
    /Month/participant_id/device_id/[csvs]

"""

import boto3
import shutil
import argparse

from csv_ingestor import CSVIngestor
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
        target_dir = zip_ref.filelist[0].filename
        target_path = os.path.join(unzipped_dir, target_dir)
        zip_ref.extractall(unzipped_dir)
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

def walking_cost(file_path, ingestor):
    """
    1. For each file path, get the file size
    2. Return the sum of all file sizes
    """
    file_paths = unzip_walk(file_path, cleanup=False)
    total_cost = 0
    for path in file_paths:
        total_cost += ingestor.estimate_csv_write_cost(file_path=path)
    return total_cost

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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file_path", help="This file will be used for ingesting records.")
    parser.add_argument("-k", "--kmsId", help="This will be used for updating the database.")
    parser.add_argument("-c", "--cost", help="Print the cost of uploading the file/s in file_path.", action=argparse.BooleanOptionalAction)
    parser.add_argument("-fs", "--full_send", help="Write the data to Timestream.", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    session = boto3.Session()

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
    if args.file_path.endswith(".zip") and args.cost:
        walking_cost(args.file_path, ingestor)
    else:
        if args.file_path is not None:
            # file_path = "data/unzipped/allsites_month/FC/157/12345ABCDE/temp.csv"
            device_id, ppt_id = extract_ids_from_path(args.file_path)
            assert len(ppt_id) == 5
            if args.cost:
                ingestor.estimate_csv_write_cost(file_path=args.file_path)
            if args.cost is None:
                ingestor.write_records_with_common_attributes(participant_id=ppt_id, device_id=device_id, file_path=args.file_path)



