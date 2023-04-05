import shutil
import unittest
import subprocess
from io import StringIO
from unittest.mock import patch
from timestone import unzip_walk, extract_streams_from_pathlist, raw_to_batch_upload


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
        file_path = 'data/unzipped/2021-07/2021-07/U01/FC/155/2M4Y4111JM/acc.csv'
        output = subprocess.run(['python', 'timestone.py', '--path', file_path], capture_output=True)
        self.assertIn('Error: You must specify a stream to ingest or all streams.', str(output.stderr))

        output_1 = subprocess.run(['python', 'timestone.py', '--path', file_path, '--streams', ' acc'], capture_output=True)
        self.assertEqual(output_1.returncode, 0)

        output_2 = subprocess.run(['python', 'timestone.py', '--path', file_path, '--all-streams'],
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

    def test_raw_to_batch(self):
        """Test that the raw_to_batch function returns the correct number of files"""
        file_path = 'test_data/zips/Sensors_U02_ALLSITES_20190801_20190831.zip'
        file_paths = unzip_walk(file_path, cleanup=False)
        file_paths = extract_streams_from_pathlist(file_paths, 'eda,acc')
        self.assertEqual(len(file_paths), 12)

        raw_to_batch_upload(file_paths, verbose=False, output_dir='./test_data/')
        shutil.rmtree('test_data/unzipped')
        # shutil.rmtree('test_data/pending_upload')

if __name__ == '__main__':
    unittest.main()
