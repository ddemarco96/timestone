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

from estimators import walking_cost, walking_time
from file_handler import unzip_walk




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



