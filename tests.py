import os
import shutil
import unittest
import subprocess
from datetime import datetime
from io import StringIO
from unittest.mock import patch
import boto3
from botocore.exceptions import ClientError

import pandas as pd
import numpy as np

from timestone import (
    unzip_walk, extract_streams_from_pathlist, raw_to_batch_format,
    create_wear_time_summary, simple_walk, handle_duplicates, recombine_cleaned_files
)
from insights import get_all_ppts, filter_ppt_list, get_ppt_df, drop_low_values, get_wear_time_by_day


class TestConvertRawToBatch(unittest.TestCase):

    def test_call_timestone(self):
        """Test that the timestone script can be called with the --help flag"""
        output = subprocess.check_output(['python', 'timestone.py', '-h'], stderr=subprocess.STDOUT, universal_newlines=True)
        self.assertIn('usage: timestone.py [-h]', output)

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

        output_1 = subprocess.run(['python', 'timestone.py', '--prep', '--path', file_path, '--streams', 'acc'], capture_output=True)
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
        # assert that there is now a combined eda file in the prepped directory
        self.assertEqual(len(os.listdir('test_data/prepped')), 1)
        self.assertEqual(os.listdir('test_data/prepped/')[0], '20190801_20190831')
        self.assertEqual(os.listdir('test_data/prepped/20190801_20190831')[0], 'eda')
        self.assertEqual(os.listdir('test_data/prepped/20190801_20190831/eda')[0], 'eda_combined_0.csv')

        df = pd.read_csv('test_data/prepped/20190801_20190831/eda/eda_combined_0.csv')
        num_lines = 9  # number of eda lines in the test data
        num_files = 6  # 2 devices for 2 ppts, 1 device for two other ppts
        self.assertEqual(df.shape[0], num_lines * num_files - 4 ) # 4 accidental duplicates in fc96 and mgh96

        shutil.rmtree('test_data/unzipped')
        shutil.rmtree('test_data/prepped')
        shutil.rmtree('test_data/cleaned_and_combined')
        os.remove('./logs/test_duplicate_log.csv')


class WearTimeTest(unittest.TestCase):

    # def test_old_wear_time(self):
    #     """Test that the wear time function returns the correct number of files"""
    #     file_path = 'data/test_wear_time.csv'
    #     output = wear_time(file_path)
    #     self.assertEqual(output.shape[0], 2)
    #     self.assertGreaterEqual(output.values[0], 15)
    #     self.assertLessEqual(output.values[0], 25)
    #     self.assertGreaterEqual(output.values[1], 30)
    #     self.assertLessEqual(output.values[1], 45)

    @patch('insights.execute_query_and_return_as_dataframe')
    def test_gets_ppt_list(self, mock_execute_query_and_return_as_df):
        """Test that the get_ppt_list function returns the correct number of files"""
        profile_name = 'nocklab'
        session = boto3.Session(profile_name=profile_name)
        query_client = session.client('timestream-query')

        # mock the execute_query_and_return_as_df function
        mock_df = pd.DataFrame({'ppt_id': ['fc100', 'mgh102', 'mgh103', 'mgh104']})
        mock_execute_query_and_return_as_df.return_value = mock_df

        ppt_list = get_all_ppts(query_client)
        self.assertEqual(len(ppt_list), 4)

    def test_list_filter(self):
        """Test that passing regex filters the participants down to those in the regex"""
        ppt_list = ['fc100', 'mgh102', 'mgh103', 'mgh104']
        regex = 'mgh'
        filtered_ppt_list = filter_ppt_list(ppt_list, regex)
        self.assertEqual(len(filtered_ppt_list), 3)
        self.assertEqual(filtered_ppt_list, ['mgh102', 'mgh103', 'mgh104'])

    def test_list_filter_no_match(self):
        """Test that passing regex filters the participants down to those in the regex"""
        ppt_list = ['fc100', 'mgh102', 'mgh103', 'mgh104']
        regex = 'MGH'
        filtered_ppt_list = filter_ppt_list(ppt_list, regex)
        self.assertEqual(len(filtered_ppt_list), 0)

    @patch('insights.execute_query_and_return_as_dataframe')
    def test_get_ppt_df(self, mock_execute_query_and_return_as_df):
        """Test that the get_ppt_df function returns the correct number of files"""
        profile_name = 'nocklab'
        session = boto3.Session(profile_name=profile_name)
        query_client = session.client('timestream-query')

        # mock the execute_query_and_return_as_df function
        mock_df = pd.DataFrame(
            {'dev_id': [
                '123ABC',
                '123ABC',
                '123ABC',
            ],
             'time': [
                 '2020-10-29 11:00:17.990000000',
                 '2020-10-29 11:00:18.240000000',
                 '2020-10-29 11:00:18.490000000',
             ],
             'value': [
                 0.000923,
                 0.012794,
                 0.001547,
             ]}
        )
        mock_execute_query_and_return_as_df.return_value = mock_df

        ppt_df = get_ppt_df(query_client, 'fc101')
        self.assertEqual(ppt_df.shape[0], 3)

    @patch('insights.execute_query_and_return_as_dataframe')
    def test_drop_low_values(self, mock_execute_query_and_return_as_df):
        """Test that the get_ppt_df function returns the correct number of files"""
        profile_name = 'nocklab'
        session = boto3.Session(profile_name=profile_name)
        query_client = session.client('timestream-query')

        # mock the execute_query_and_return_as_df function
        # create a mock dataframe with 1000 rows, 300 of which are below 0.03
        mock_df = pd.DataFrame({
            'dev_id': ['123ABC'] * 1000,
            'time': pd.date_range('2020-10-29 11:00:17.990000000', periods=1000, freq='s'),
            'value': [0.000923] * 300 + [0.12794] * 700
        })

        mock_execute_query_and_return_as_df.return_value = mock_df

        old_df = get_ppt_df(query_client, 'fc101')
        new_df, start_len, end_len = drop_low_values(old_df, ppt_id='fc101', output_dir='.', threshold=0.03)
        self.assertEqual(old_df.shape[0], 1000)
        self.assertEqual(new_df.shape[0], 700)
        self.assertEqual(start_len, 1000)
        self.assertEqual(end_len, 700)

    @patch('insights.execute_query_and_return_as_dataframe')
    def test_generate_summary(self, mock_execute_query_and_return_as_df):
        """Test that the get_ppt_df function returns the correct number of files"""
        profile_name = 'nocklab'
        session = boto3.Session(profile_name=profile_name)
        query_client = session.client('timestream-query')

        # mock the execute_query_and_return_as_df function
        # create a mock dataframe with 1000 rows, 300 of which are below 0.03
        mock_df = pd.DataFrame({
            'dev_id': ['123ABC'] * 1000,
            'time': pd.date_range('2020-10-29 11:00:17.990000000', periods=1000, freq='s'),
            'value': [0.000923] * 300 + [0.12794] * 700
        })

        mock_execute_query_and_return_as_df.return_value = mock_df

        old_df = get_ppt_df(query_client, 'fc101')
        new_df, start_len, end_len = drop_low_values(old_df, ppt_id='fc101', output_dir='.', threshold=0.03)
        summary_df = get_wear_time_by_day(new_df)
        self.assertEqual(summary_df.shape[0], 1)

        # divide by 4 because we have "4hz" measures every second
        self.assertEqual(summary_df.iloc[0]['minutes_worn'], 700 / 60 / 4)
        self.assertEqual(summary_df.iloc[0]['percent_worn'], 700 / 86400 / 4)

class SimpleWalkTestCase(unittest.TestCase):
    def setUp(self):
        # Define the directory path and create sample files
        self.dir_path = './test_data/simple_walk'
        os.makedirs(self.dir_path, exist_ok=True)
        open(os.path.join(self.dir_path, 'eda.csv'), 'w').close()
        open(os.path.join(self.dir_path, 'temp.csv'), 'w').close()
        open(os.path.join(self.dir_path, 'acc.csv'), 'w').close()
        open(os.path.join(self.dir_path, 'other.csv'), 'w').close()

    def tearDown(self):
        # Remove the sample files and directory
        os.remove(os.path.join(self.dir_path, 'eda.csv'))
        os.remove(os.path.join(self.dir_path, 'temp.csv'))
        os.remove(os.path.join(self.dir_path, 'acc.csv'))
        os.remove(os.path.join(self.dir_path, 'other.csv'))
        os.rmdir(self.dir_path)

    def test_simple_walk(self):
        # Define the test case
        expected_paths = [
            '/test_data/eda.csv',
            '/test_data/temp.csv',
            '/test_data/acc.csv']
        result_paths = simple_walk(self.dir_path)
        self.assertEqual(result_paths.sort(), expected_paths.sort())

class TestDuplicateHandling(unittest.TestCase):
    def setUp(self):
        # Define the directory path and create sample files
        self.dir_path = './test_data/duplicate_handling'
        os.makedirs(self.dir_path, exist_ok=True)
        os.makedirs(os.path.join(self.dir_path, 'prepped'), exist_ok=True)
        """
        create a mock dataframe with 1000 rows, 
           400 for ppt_id 1001 (dev_id 123ABC) and 600 for ppt_id 1002 (dev_id 456DEF)
           values can be a random number between 0 and 1
        """
        self.mock_df = pd.DataFrame({
            'time': pd.date_range('2020-10-29 11:00:17.990000000', periods=1000, freq='s'),
            'measure_value': np.random.rand(1000),
            'dev_id': ['123ABC'] * 400 + ['456DEF'] * 600,
            'ppt_id': ['ppt_1001'] * 400 + ['ppt_1002'] * 600,
        })
        # duplicate (perfectly identical) the last 200 rows of the df
        self.mock_df = pd.concat([self.mock_df, self.mock_df.tail(100).copy()], ignore_index=True)
        # concat 200 rows for ppt 1001 (dev_id 123ABC) that are duplicates of the first 200 rows in terms of time but
        # have different measure values (dupes_unclear)
        self.mock_df = pd.concat([self.mock_df, pd.DataFrame({
            'time': pd.date_range('2020-10-29 11:00:17.990000000', periods=200, freq='s'),
            'measure_value': np.random.rand(200),
            'dev_id': ['123ABC'] * 200,
            'ppt_id': ['ppt_1001'] * 200,
        })])
        self.mock_df = pd.concat([self.mock_df, pd.DataFrame({
            'time': pd.date_range('2020-10-29 11:00:17.990000000', periods=300, freq='s'),
            'measure_value': np.NaN,
            'dev_id': ['123ABC'] * 300,
            'ppt_id': ['ppt_1001'] * 300,
        })])

        # convert the time column to timestamp format
        self.mock_df['time'] = pd.to_datetime(self.mock_df['time']).astype('int64') // 10 ** 9


        # save the df to a test csv file
        self.df_path = os.path.join(self.dir_path, 'prepped', 'test_eda_combined_0.csv')
        self.mock_df.to_csv(self.df_path, index=False)


    def tearDown(self):
        # Remove the sample files and directory
        # os.remove(os.path.join(self.dir_path, 'test_eda_combined_0.csv'))
        # remove the test_log file if it exists
        os.remove('./logs/test_duplicate_log.csv')
        # shutil.rmtree(self.dir_path)
    def test_duplicate_handling(self):
        # test that duplicates are detected and removed from the csv file
        path = self.df_path

        handle_duplicates(file_paths=[path], scan_only=False, verbose=False)
        df = pd.read_csv('./logs/test_duplicate_log.csv')
        current_log = df.loc[df['path'] == path].iloc[0].to_dict()
        # check if the file has the correct number of rows (base + perf + unclear + nan)
        # breakpoint()
        self.assertEqual(current_log['total_rows'], 1000 + 100 + 200 + 300)
        # check if the file has the correct number of duplicates
        self.assertEqual(current_log['total_dupes'], 600)

        self.assertEqual(current_log['perfect'], 100)
        self.assertEqual(current_log['unclear'], 200 + 300) # unclear with values + unclear with nan
        self.assertEqual(current_log['nan'], 300)

        # check if the log has a note of which participants were removed and how many duplicates were found
        self.assertEqual(current_log['dupe_ppts'], ',ppt_1002,ppt_1001')

    def test_recombination(self):
        """Assert that after droping duplicates we can recombine the files without losing any additional data"""
        path = self.df_path
        handle_duplicates(file_paths=[path], scan_only=False, verbose=False)
        df_size = self.mock_df.memory_usage(deep=True).sum()
        max_for_3_files = df_size // 3

        # split the mock df into 2 files to be recombined
        df = pd.read_csv(path)
        df1 = df.iloc[:len(df) // 2]
        df2 = df.iloc[len(df) // 2:]
        df1_path = os.path.join(self.dir_path, 'prepped', 'test_eda_combined_0.csv')
        df2_path = os.path.join(self.dir_path, 'prepped', 'test_eda_combined_1.csv')
        df1.to_csv(df1_path, index=False)
        df2.to_csv(df2_path, index=False)

        # recombine the files and assert that the recombined file has the same number of rows as the original
        recombine_cleaned_files(file_paths=[df1_path, df2_path], max_size=max_for_3_files, output_dir=os.path.join(self.dir_path, 'cleaned_and_combined')) # 100 byte max size should split the file into 3
        # assert that there are 3 files

        recombined_files = simple_walk(os.path.join(self.dir_path, 'cleaned_and_combined'))
        recombined_num_rows = 0
        for p in recombined_files:
            recombined_num_rows += pd.read_csv(p).shape[0]
        self.assertEqual(recombined_num_rows, df.shape[0])
        self.assertEqual(len(simple_walk(os.path.join(self.dir_path, 'cleaned_and_combined'))), 2)

if __name__ == '__main__':
    unittest.main()
