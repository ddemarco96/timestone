# Timestone

## Introduction

`timestone.py` is a command-line interface (CLI) tool designed to upload data to Amazon Timestream. It helps with the preparation of data and analytics of insights.

## Prerequisites

-   `.env` file: A file that holds environment variables, such as your database connection details. Due to containing sensitive information, this file is typically not shared or checked into version control.
-   Python3 installed and configured in your environment.
-   Certain Python packages installed: pandas, dotenv, etc. 
-   AWS CLI installed and configured in your environment.
-   AWS Timestream database and table created. 
-   if using the default AWS targets, an EC2 instance with the following installed:
    -   Python3
    -   AWS CLI
    -   AWS Timestream Write
    -   a set of aws configuration files (credentials, config, etc.) with a profile named `nocklab`

**N.B.** Timestone is currently only tested on MacOS. Additional testing and configuration may be required for Windows and Linux.
## Quick Start Usage

Make sure you've run `make setup` to create the required folder structure and `.env` file. 
Assuming your original embrace data is in the `data/zips` folder, run the following command to upload the data to Timestream:

```bash
make <target> filename='<YourFileName>.zip'
```
Replace `filename='<YourFileName>.zip'` at the end of the command with the file name that you wish to process with this pipeline.
Notice `<target>` can be one of the following:

1. `all`: Runs prep, gzip, and upload commands sequentially.
2. `prep`: Uses `timestone.py` to process the data file.
3. `gzip`: Compresses the directory (gzip), and calculates and prints the size difference.
4. `upload`: Uploads the gzipped file to the AWS EC2 server. After a successful upload, it sends a notification.
#### filename is not required for the following targets:
5. `setup`: Sets up the folder structure and the .env file.
6. `help`: Shows the list of available make commands.

## Folder Structure

The `setup` command will create the required folder structure, as follows:

-   data/zips
-   data/Stage3-combined_and_ready_for_upload
-   data/Stage2-deduped_eda_cleaned
-   data/unzipped
-   logs

After the setup, update your `.env` file with the `SLACK_WEBHOOK_URL` variable to facilitate sending notifications to Slack when an EC2 upload completes.

## Timestone.py CLI

#### For a more fine-tuned usage than the makefile targets, you can use the `timestone.py` CLI directly.

`timestone.py` provides multiple options and arguments. Here's a brief on those arguments:

-  `--path`: Path to the folder having the data to upload.
-  `--ppts`, `--list-filter`: Options to filter the data for specific participant.
-  `-o`, `--output`: Output path for the data.
-  `-db`, `--database`: Name of the database to upload.
-  `-t`, `--table`: Name of the table to upload.
-  `-bn`, `--bucket-name`: Name of S3 bucket for upload.
-  `-u`, `--upload`: Option to upload the data to Timestream via API.
-  `-p`, `--prep`: Prepare the data for bulk upload via S3.
-  `-v`, `--verbose`: Prints out extra information.
-  `-n`, `--dry-run`: Prints the cost of upload without actual uploading.
-  `-s`, `--streams`, `-as`, `--all-streams`: Specify the streams to ingest.
-  `-i`, `--insights`: Calculates insights for the data.
-  `--cleanup`: Remove the unzipped files after uploading.
-  `--create`: Creates the bucket before uploading.


## Limitations

Running these scripts will hog a LOT of memory and CPU. It is recommended to run these scripts on a machine with at least 16GB of RAM and 8 cores.
Expect a 6x increase in the size of the decompressed data which is copied through each of the stages by default. So,
-   2GB of compressed data in Sharepoint will lead to at least 36GB of additional data added to the directory structure.

The purpose of this insane inefficiency is to be able to document and test the data at each stage of the pipeline.
Once tested, just retain the gzip and delete the new directories in Stage 2 and Stage 3, and data/unzipped.

The pipeline assumes having the necessary permissions for accessing the target EC2 instance and the required Python dependencies for executing the `timestone.py` script. To view help on commands or for issues, run:

```bash
make help 
```
or 
```bash
python3 timestone.py --help
```