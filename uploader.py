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


def unzip_walk(filepath, cleanup=True):
    """
    1. Find the parent dir of the filepath
    2. Unzip the file
    3. Move the contents to parent_dir/unzipped
    4. return the list of file paths to any eda, temp, or acc csvs files in any dir within the unzipped dir
    """
    # 1. Find the parent dir of the filepath
    grandparent_dir = os.path.dirname(os.path.dirname(filepath))
    unzipped_dir = os.path.join(grandparent_dir, "unzipped")
    if not os.path.exists(unzipped_dir):
        os.mkdir(unzipped_dir)
    # 2/3. Unzip the file and move all to unzipped_dir
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    # 4. return the list of file paths to any eda, temp, or acc csvs files in any dir within the unzipped dir
    file_paths = []
    for root, dirs, files in os.walk(unzipped_dir):
        for file in files:
            if file.endswith(".csv"):
                file_paths.append(os.path.join(root, file))
    # cleanup by removing the unzipped dir if you want
    if cleanup:
        shutil.rmtree(unzipped_dir)
    return file_paths




if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filepath", help="This file will be used for ingesting records")
    parser.add_argument("-k", "--kmsId", help="This will be used for updating the database")
    parser.add_argument("-c", "--cost", help="Print the cost of uploading the file in filepath", action=argparse.BooleanOptionalAction)
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

    if args.filepath is not None:
        # filepath = "data/unzipped/allsites_month/FC/157/12345ABCDE/temp.csv"
        path_list = args.filepath.split('/')
        device_id = path_list[-2]
        ppt_id = path_list[-4].lower() + path_list[-3]
        assert len(ppt_id) == 5
    if args.cost:
        ingestor.estimate_csv_write_cost(participant_id=ppt_id, device_id=device_id, filepath=args.filepath)
    if args.cost is None:
        ingestor.write_records_with_common_attributes(participant_id=ppt_id, device_id=device_id, filepath=args.filepath)



