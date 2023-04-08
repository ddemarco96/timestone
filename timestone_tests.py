import os
import shutil
import unittest
import subprocess
from datetime import datetime
from io import StringIO
from unittest.mock import patch

import pandas as pd

from timestone import unzip_walk, extract_streams_from_pathlist, raw_to_batch_format, wear_time


class TestConvertRawToBatch(unittest.TestCase):
    def test_call_timestone(self):
        """Test that the timestone script can be called with the --help flag"""
        subprocess.check_output(['python', 'timestone.py', '--h'], stderr=subprocess.STDOUT, universal_newlines=True)

    def test_needs_path(self):
        """Test that the timestone script requires a path to a directory"""
        # with patch('sys.stdout', new=StringIO()) as fake_out:
        output = subprocess.run(['python', 'timestone.py'], capture_output=True)
        self.assertIn('Error: Please provide a path to the data to upload', str(output.stderr))
    def test_needs_stream(self):
        """Test that the timestone script requires a stream name"""
        file_path = 'test_data/zips/Sensors_U02_ALLSITES_20190801_20190831.zip'
        output = subprocess.run(['python', 'timestone.py', '--prep', '--path', file_path], capture_output=True)
        self.assertIn('Error: You must specify a stream to ingest or all streams.', str(output.stderr))

        output_1 = subprocess.run(['python', 'timestone.py', '--prep', '--path', file_path, '--streams', ' acc'], capture_output=True)
        self.assertEqual(output_1.returncode, 0)

        output_2 = subprocess.run(['python', 'timestone.py', '--prep', '--path', file_path, '--all-streams'],
                                  capture_output=True)
        self.assertEqual(output_2.returncode, 0)

class TestFileHandlers(unittest.TestCase):
    # def test_unzip_walk(self):
    #     """Test that the unzip_walk function returns the correct number of files"""
    #     file_path = 'data/zips/2021-07.zip'
    #     file_paths = unzip_walk(file_path, cleanup=False)
    #     self.assertEqual(len(file_paths), 3)
    #
    # def test_extract_streams_from_pathlist(self):
    #     """Test that the extract_streams_from_pathlist function returns the correct number of files"""
    #     file_path = 'data/zips/2021-07.zip'
    #     file_paths = unzip_walk(file_path, cleanup=False)
    #     file_paths = extract_streams_from_pathlist(file_paths, 'acc')
    #     self.assertEqual(len(file_paths), 1)

    def test_raw_to_batch_runs(self):
        """Test that the raw_to_batch function returns the correct number of files"""
        file_path = 'test_data/zips/Sensors_U02_ALLSITES_20190801_20190831.zip'
        file_paths = unzip_walk(file_path, cleanup=False)
        streams = 'eda'
        file_paths = extract_streams_from_pathlist(file_paths, streams)
        self.assertEqual(len(file_paths), len(streams.split(',')) * 6)

        raw_to_batch_format(file_paths, verbose=False, output_dir='./test_data/', streams=streams)
        # assert that there is now a combined eda file in the pending_upload directory
        self.assertEqual(len(os.listdir('test_data/pending_upload')), 1)
        self.assertEqual(os.listdir('test_data/pending_upload/')[0], '20190801_20190831')
        self.assertEqual(os.listdir('test_data/pending_upload/20190801_20190831')[0], 'eda')
        self.assertEqual(os.listdir('test_data/pending_upload/20190801_20190831/eda')[0], 'combined_0.csv')

        df = pd.read_csv('test_data/pending_upload/20190801_20190831/eda/combined_0.csv')
        num_lines = 9  # number of eda lines in the test data
        num_files = 6  # 2 devices for 2 ppts, 1 device for two other ppts
        self.assertEqual(df.shape[0], num_lines * num_files)

        shutil.rmtree('test_data/unzipped')
        shutil.rmtree('test_data/pending_upload')


class WearTimeTest(unittest.TestCase):

    def test_wear_time(self):
        """Test that the wear time function returns the correct number of files"""
        file_path = 'data/test_wear_time.csv'
        output = wear_time(file_path)
        self.assertEqual(output.shape[0], 2)
        self.assertGreaterEqual(output.values[0], 15)
        self.assertLessEqual(output.values[0], 25)
        self.assertGreaterEqual(output.values[1], 30)
        self.assertLessEqual(output.values[1], 45)


if __name__ == '__main__':
    unittest.main()
