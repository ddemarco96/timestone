import os
import sys
import threading

import boto3
from botocore.exceptions import ClientError

def create_bucket(bucket_name, profile_name=None):
    # upload the files to s3
    print(f"Creating an s3 bucket: {bucket_name}")
    profile_name = profile_name if profile_name else 'nocklab'
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)
    print("Bucket Created")
    return s3_client

def get_client(profile_name=None):
    profile_name = profile_name if profile_name else 'nocklab'
    session = boto3.Session(profile_name=profile_name)
    return session.client('s3')

def upload_to_s3(file_paths, bucket_name, s3_client):
    for path in file_paths:
        object_name = os.path.basename(path)
        print(f"Uploading files in: {path}")
        try:
            s3_client.upload_file(
                path,
                bucket_name,
                object_name,
                Callback=ProgressPercentage(path)
            )
        except ClientError as e:
            print(e)
            return False
    return True


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()