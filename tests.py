from unittest import main, TestCase
from unittest.mock import patch
from io import StringIO

from csv_ingestor import CSVIngestor


class TestGetFileInfo(TestCase):
    def test_get_dimension_info(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            filepath = "./data/fc155_2022-07/2M4Y4111JM/temp.csv"
            path_list = filepath.split('/')
            device_id = path_list[-2]
            ppt_id = path_list[2].split('_')[0]
            CSVIngestor(None).write_records_with_common_attributes(
                                                            participant_id=ppt_id,
                                                            device_id=device_id,
                                                            filepath=filepath)
            msg = f"Writing records and extracting common attributes for {ppt_id}..."
            self.assertIn(msg, fake_out.getvalue())


main()
