"""A CLI for uploading data to timestream"""
import argparse
import boto3
from botocore.config import Config
import os
import shutil
import zipfile

from csv_ingestor import CSVIngestor
from file_handler import unzip_walk, extract_streams_from_pathlist, raw_to_batch_format
from estimators import  walking_cost, walking_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Tools for helping to upload embrace data to Amazon Timestream')
    # parser.add_argument('-h', '--help', action='help', help='Show this help message and exit')
    parser.add_argument('--path', type=str, help='Path to the folder containing the data to upload')
    parser.add_argument('-db', '--database', type=str, help='Name of the database to upload to')
    parser.add_argument('-t', '--table', type=str, help='Name of the table to upload to')
    parser.add_argument('-u', '--upload', action='store_true', help='Upload the data to Timestream via API')
    parser.add_argument('-p', '--prep', action='store_true', help='Prepare the data for bulk upload via S3')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print out extra information')
    parser.add_argument('-n', '--dry-run', action='store_true', help='Print out the cost of the upload without actually uploading')
    parser.add_argument("-s", "--streams", help="Comma separated list of streams to ingest e.g., 'acc,temp'.")
    parser.add_argument("-as", "--all-streams", help="Ingest all streams, ignore -s/--streams.",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('--cleanup', action='store_true', help='Remove the unzipped files after uploading')
    args = parser.parse_args()



    ## Functionalities
    # estimate the costs of the upload via api
    # upload via api
    # prep for bulk upload via s3
    # upload via s3

    if args.path is None:
        raise ValueError("Please provide a path to the data to upload")

    # check if the file_path is a zip file
    if args.path.endswith(".zip"):
        # if it is, unzip it and get the file paths to all the csvs
        file_paths = unzip_walk(args.path, cleanup=False)
    else:
        # if it's not, just get the file path
        file_paths = [args.path]

    # filter the path list based on --as or -s
    if not args.all_streams:
        if args.streams is None:
            raise ValueError("You must specify a stream to ingest or all streams.")
        else:
            file_paths = extract_streams_from_pathlist(file_paths, args.streams)

    if args.prep:
        print("Prepping files for bulk upload")
