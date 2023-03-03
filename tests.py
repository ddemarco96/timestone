from sys import getsizeof
from unittest import main, TestCase
from unittest.mock import patch
from io import StringIO

from csv_ingestor import CSVIngestor
<<<<<<< Updated upstream
=======
from uploader import unzip_walk, extract_streams_from_pathlist
>>>>>>> Stashed changes


class TestGetFileInfo(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.filepath = "data/unzipped/allsites_month/FC/157/2M4Y4111FK/temp.csv"
        cls.path_list = cls.filepath.split('/')
        cls.device_id = cls.path_list[-2]
        cls.ppt_id = cls.path_list[-4].lower() + cls.path_list[-3]
        cls.common_attrs = CSVIngestor(None).get_common_attrs(cls.filepath, cls.ppt_id, cls.device_id)
        cls.df = CSVIngestor(None).get_timestream_df(cls.filepath)

    def test_get_common_attrs(self):
        dims = self.common_attrs['Dimensions']
        self.assertEqual(len(dims), 2)
        self.assertEqual(dims[0]['Value'], self.ppt_id)
        self.assertEqual(dims[1]['Value'], self.device_id)
        self.assertEqual(self.common_attrs['MeasureValueType'], 'DOUBLE')
        # TODO: expand this to test all the different types of csvs
        self.assertEqual(self.common_attrs['MeasureName'], 'temp_degC')

<<<<<<< Updated upstream
    def test_get_timestream_df(self):
        self.assertEqual(self.df.shape[1], 2)
        self.assertEqual(self.df.columns[0], 'Time')
        self.assertEqual(self.df.columns[1], 'MeasureValue')

    def test_get_optimal_batch_size(self):
        batch_size = CSVIngestor(None).get_optimal_batch_size(self.df, self.common_attrs)
        self.assertLessEqual(batch_size, 1000)
        self.assertGreaterEqual(batch_size, 1)
        write_max = 1000
        # 232 bytes for common attributes
        common_attr_size = getsizeof(self.common_attrs)
        # 16 bytes per row
        avg_row_size = round(self.df.memory_usage(deep=True).sum() / self.df.shape[0])
        # (1000 - 232) / 16 = 48
        batch_size = (write_max - common_attr_size) // avg_row_size
        # for this csv we know the optimal batch size is 48
        self.assertEqual(batch_size, 48)

    def test_get_cost_info(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:

            cost = CSVIngestor(None).estimate_csv_write_cost(
                                                    participant_id=self.ppt_id,
                                                    device_id=self.device_id,
                                                    filepath=self.filepath)
            msg = f"Estimated cost for {self.filepath}: $"
            self.assertLessEqual(cost, 1.00) # a single temp csv should not cost more than $1
            self.assertIn(msg, fake_out.getvalue())

    def test_write_records(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            CSVIngestor(None).write_records_with_common_attributes(
                                                    participant_id=self.ppt_id,
                                                    device_id=self.device_id,
                                                    filepath=self.filepath)
            msg = f"Writing records and extracting common attributes for {self.ppt_id}..."
            self.assertIn(msg, fake_out.getvalue())
=======
    def test_csv_num_rows(self):
        expected_rows = 1940589  # CSVIngestor(None).get_timestream_df(cls.file_path).shape[0]
        subprocess_rows = CSVIngestor(None).get_num_rows(self.file_path)
        self.assertEqual(expected_rows, subprocess_rows)

    def test_get_optimal_writes_per_request(self):
        csv_types = ["temp", "eda", "acc"]
        params_by_type = {
            "temp": {
                "common_attr_size": 36,
                "record_size": 16,
                "expected_best": round(100/60)  # =  round(100/((1000-36)//16))
            },
            "eda": {
                "common_attr_size": 37,
                "record_size": 16,
                "expected_best": round(100/60)  # =  round(100/((1000-37)//16))
            },
            "acc": {
                "common_attr_size": 32,
                "record_size": 32,
                "expected_best": round(100/30)  # =  round(100/((1000-32)//32))
            },
        }
        for t in csv_types:
            expected_best = params_by_type[t]["expected_best"]
            file_path = f"FC/155/2M4Y4111JM/{t}.csv"
            self.assertEqual(CSVIngestor(None).get_optimal_writes_per_request(file_path), expected_best)

    def test_get_cost_info(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            df_rows = CSVIngestor(None).get_num_rows(self.file_path)
            cost = CSVIngestor(None).estimate_csv_write_cost(file_path=self.file_path, df_rows=df_rows, verbose=True)
            msg = f"Estimated cost for {self.file_path}: $"
            self.assertLessEqual(cost, 1.00)  # a single temp csv should not cost more than $1
            self.assertIn(msg, fake_out.getvalue())

    def test_get_time_info(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            expected_rows = 1940589
            df_rows = CSVIngestor(None).get_num_rows(self.file_path)
            minutes = CSVIngestor(None).estimate_csv_write_time(file_path=self.file_path, df_rows=df_rows, verbose=True)
            est_minutes = round(expected_rows/1000000 * 11, 2)
            msg = f"Estimated time for {self.file_path}: {est_minutes} minutes"
            # almost 2M rows should take ~22 minutes to write
            self.assertLessEqual(minutes, 22.00)
            self.assertGreaterEqual(minutes, 11.00)
            self.assertIn(msg, fake_out.getvalue())
    #
    def test_unzip_walk(self):
        # with patch('sys.stdout', new=StringIO()) as fake_out:
        test_file_path = "test_data/zips/Sensors_U02_ALLSITES_20190801_20190831.zip"
        file_paths = unzip_walk(test_file_path, cleanup=True)
        expected_paths = [
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/eda.csv',
        ]
        self.assertTrue(all([path in file_paths for path in expected_paths]))
        self.assertFalse(os.path.exists("test_data/unzipped/"))

    def test_unzip_walk_no_cleanup(self):
        test_file_path = "test_data/zips/Sensors_U02_ALLSITES_20190801_20190831.zip"
        file_paths = unzip_walk(test_file_path, cleanup=False)
        expected_paths = [
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/eda.csv',
        ]
        self.assertTrue(all([path in file_paths for path in expected_paths]))
        self.assertTrue(os.path.exists("test_data/unzipped/"))
        shutil.rmtree("test_data/unzipped/")

    def test_path_filtering(self):
        streams = 'eda,temp'
        base_paths = [
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/acc.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/eda.csv',
        ]
        filtered_paths = extract_streams_from_pathlist(base_paths, streams)
        expected_paths = [
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/FC/157/12345ABCDE/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/096/2M4Y4111FK/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/ABCDE12345/eda.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/temp.csv',
            'test_data/unzipped/Sensors_U02_ALLSITES_20190801_20190831/U02/MGH/157/12345ABCDE/eda.csv',
            ]
        self.assertEqual(filtered_paths, expected_paths)


    # very slow, uncomment to test
    # def test_cost_with_zip(self):
    #     with patch('sys.stdout', new=StringIO()) as fake_out:
    #         ingestor = CSVIngestor(None)
    #         total_cost = walking_cost(self.zippath, ingestor)
    #         self.assertGreaterEqual(total_cost, 0.0)
    #         self.assertEqual(round(total_cost, 2), round(1.0489685, 2))
    #         shutil.rmtree("data/unzipped/2021-07")

    @patch('boto3.Session')
    def test_write_records(self, Session):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            mock_session = Session.return_value
            client = mock_session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
            client.write_records = MagicMock(return_value={'ResponseMetadata': {'HTTPStatusCode': 200}})

            num_read = CSVIngestor(client).write_records_with_common_attributes(
                participant_id=self.ppt_id,
                device_id=self.device_id,
                file_path=self.file_path,
                verbose=True)
            # Check that the file was looped through
            num_rows = CSVIngestor(None).get_num_rows(self.file_path)
            num_chunks = num_rows // 1000000
            self.assertEqual(num_read, num_rows)
            chunk_msg = f"Processing chunk {num_chunks} with 1000000 records..."
            self.assertIn(chunk_msg, fake_out.getvalue())


>>>>>>> Stashed changes

main()
