import csv
import time
from constants import DATABASE_NAME, TABLE_NAME


class CSVIngestor:
    """
    CSVs need to be read and processed into the right format before they can actually be uploaded.

    This class takes a standard embrace csv for any of the streams and first generates the record formatting timestream
    expects. Once formatted, records are written in batches of 100 at a time.

    Terms to Know:
    - Dimension
        these are the properties of an entry which we expect to be very repetitive (e.g., device/participant id)
    - Measure
        the thing actually recorded by the sensor. E.g., for temperature (in C)
            measure name = temp_degC, measure value = 38.123, measureValueType = "DOUBLE"
    """
    def __init__(self, client):
        self.client = client

    def write_records_with_common_attributes(self, participant_id, device_id, filepath):
        print(f"Writing records and extracting common attributes for {participant_id}...")

        # Dimensions are the things we expect to be constant across many rows (e.g., device/participant id)
        dimensions = [
            # {'Name': 'region', 'Value': 'us-east-1'},
            # {'Name': 'az', 'Value': 'az1'},
            # {'Name': 'hostname', 'Value': 'host1'},
            {'Name': 'ppt_id', 'Value': participant_id},
            {'Name': 'device_id', 'Value': device_id}
        ]

        measure_name = None
        if "eda.csv" in filepath:
            measure_name = "eda_microS"
        elif "temp.csv" in filepath:
            measure_name = "temp_degC"
        assert measure_name is not None

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'MeasureName': measure_name,
        }

        cpu_utilization = {
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5'
        }




    @staticmethod
    def _current_milli_time():
        return int(round(time.time() * 1000))