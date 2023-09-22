"""A CLI for uploading data to timestream"""
import argparse
import os
import shutil
import zipfile
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from csv_ingestor import CSVIngestor
from file_handler import (
    unzip_walk, extract_streams_from_pathlist, raw_to_batch_format, simple_walk, handle_duplicates,
    send_slack_notification, combine_files_and_add_columns, copy_files_to_stage2
)
from insights import create_wear_time_summary, get_all_ppts
from uploader import create_bucket, upload_to_s3, get_client
from estimators import  walking_cost, walking_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Tools for helping to upload embrace data to Amazon Timestream')
    # parser.add_argument('-h', '--help', action='help', help='Show this help message and exit')
    parser.add_argument('--path', type=str, help='Path to the folder containing the data to upload')
    parser.add_argument('--ppts', type=str, help='Comma separated list of ppts to upload e.g., "1,2,3"')
    parser.add_argument('--list-filter', type=str, help='Regex pattern to reduce the list of ppts to upload e.g., "1[0-9]')
    parser.add_argument('-o', '--output', type=str, help='Path to the folder to output the data to')
    parser.add_argument('-db', '--database', type=str, help='Name of the database to upload to')
    parser.add_argument('-t', '--table', type=str, help='Name of the table to upload to')
    parser.add_argument('-bn', '--bucket-name', type=str, help='Name of the s3 bucket to upload to')
    parser.add_argument('-u', '--upload', action='store_true', help='Upload the data to Timestream via API')
    parser.add_argument('-p', '--prep', action='store_true', help='Prepare the data for bulk upload via S3')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print out extra information')
    parser.add_argument('-n', '--dry-run', action='store_true', help='Print out the cost of the upload without actually uploading')
    parser.add_argument("-s", "--streams", help="Comma separated list of streams to ingest e.g., 'acc,temp'.")
    parser.add_argument("-as", "--all-streams", help="Ingest all streams, ignore -s/--streams.",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('-i', '--insights', action='store_true', help='Calculate insights for the data')
    parser.add_argument('--cleanup', action='store_true', help='Remove the unzipped files after uploading')
    parser.add_argument('--create', action='store_true', help="Create the bucket before uploading")
    args = parser.parse_args()



    ## Functionalities
    # estimate the costs of the upload via api
    # upload via api
    # prep for bulk upload via s3
    # upload via s3

    if args.path is None and not args.insights:
        raise ValueError("Please provide a path to the data to upload")

    if args.path is not None:
        # check if the file_path is a zip file
        if args.path.endswith(".zip"):
            # if it is, unzip it and get the file paths to all the csvs
            file_paths = unzip_walk(args.path, cleanup=False)
        # if the path is a directory
        elif os.path.isdir(args.path):
            # walk without unzipping
            file_paths = simple_walk(args.path)
        else:
            # if it's not, just get the file path
            file_paths = [args.path]

        if not args.all_streams and not args.streams and not args.insights:
                raise ValueError("You must specify a stream to ingest or all streams.")
        streams = args.streams if args.streams is not None else 'acc,eda,temp'
        print(f"Streams to ingest: {streams}"
              f"\nAll streams: {args.all_streams}"
              f"\nInsights: {args.insights}")
        if not args.upload:
            file_paths = extract_streams_from_pathlist(file_paths, streams)


    if args.prep:
        # filter the path list based on --as or -s
        print("Prepping files for bulk upload")
        # prep the files for bulk upload
        output = args.output if args.output else '.'
        raw_to_batch_format(file_paths, streams=streams, verbose=args.verbose, output_dir=output)

    # if args.insights:
    #     if args.prep:
    #         raise ValueError("You have to prep and get insights in two different calls. "+
    #                          "Don't forget to switch the path to the Stage2-deduped_eda_cleaned files")
    #     file_paths = extract_streams_from_pathlist(file_paths, 'eda')
    #     for path in file_paths:
    #         print(path)
    #         print(wear_time(path))

    if args.upload:
        if args.bucket_name is None:
            raise ValueError("Please provide a name for the Bucket to create")

        if args.create:
            # create the bucket
            s3_client = create_bucket(args.bucket_name)
        else:
            # get the client
            s3_client = get_client("nocklab")
        # upload the files to the bucket
        upload_to_s3(file_paths, args.bucket_name, s3_client)
        send_slack_notification(f"Uploaded {len(file_paths)} files to {args.bucket_name}")


    if args.insights:
        # use input to check whether they want a summary of wear time or list of participants
        choice = input("Do you want a summary of wear time or a list of participants? (s/l) ")
        if choice == 's':
            # save the wear time to a csv
            create_wear_time_summary(
                ppt_list=args.ppts.split(','),
                list_filter=args.list_filter,
                output_dir=args.output,
                save=True)
        if choice == 'l':
            # print the list of participants
            print(get_all_ppts())