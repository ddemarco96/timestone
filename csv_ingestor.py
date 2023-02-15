import csv
import time
from constants import DATABASE_NAME, TABLE_NAME


class CSVIngestor:
    def __init__(self, client):
        self.client = client

    def write_records_with_common_attributes(self, participant_id, device_id, filepath):
        print("Writing records extracting common attributes")
        current_time = self._current_milli_time()

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

        memory_utilization = {
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40'
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def bulk_write_records(self, filepath):
        with open(filepath, 'r') as csv_file:
            # creating a csv reader object
            csv_reader = csv.reader(csv_file)

            records = []
            current_time = self._current_milli_time()
            counter = 0

            # extracting each data row one by one
            for row in csv_reader:
                dimensions = [
                    {'Name': row[0], 'Value': row[1]},
                    {'Name': row[2], 'Value': row[3]},
                    {'Name': row[4], 'Value': row[5]}
                ]

                record_time = current_time - (counter * 50)

                record = {
                    'Dimensions': dimensions,
                    'MeasureName': row[6],
                    'MeasureValue': row[7],
                    'MeasureValueType': row[8],
                    'Time': str(record_time)
                }

                records.append(record)
                counter = counter + 1

                if len(records) == 100:
                    self._submit_batch(records, counter)
                    records = []

            if len(records) != 0:
                self._submit_batch(records, counter)

            print("Ingested %d records" % counter)

    def _submit_batch(self, records, counter):
        try:
            result = self.client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                               Records=records, CommonAttributes={})
            print("Processed [%d] records. WriteRecords Status: [%s]" % (counter,
                                                                         result['ResponseMetadata']['HTTPStatusCode']))
        except Exception as err:
            print("Error:", err)

    @staticmethod
    def _current_milli_time():
        return int(round(time.time() * 1000))