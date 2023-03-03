from sys import getsizeof
from unittest import main, TestCase
from unittest.mock import patch
from io import StringIO

from csv_ingestor import CSVIngestor


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

main()
