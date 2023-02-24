import os
import subprocess
import time
import pandas as pd
from sys import getsizeof
from constants import DATABASE_NAME, TABLE_NAME


class CSVIngestor:
    """
    CSVs need to be read and processed into the right format before they can actually be uploaded.

    This class takes a standard embrace csv for any of the streams and first generates the record formatting timestream
    expects. Once formatted, records are written in batches of 100 at a time.

    Terms to know:

    Time series - A sequence of one or more data points (or records) recorded over a time interval.
        Examples are the price of a stock over time, the CPU or memory utilization of an EC2 instance over time, and
        the temperature/pressure reading of an IoT sensor over time.
    Record - A single data point in a time series.
    Dimension - An attribute that describes the meta-data of a time series.
        A dimension consists of a dimension name and a dimension value. Consider the following examples:
            When considering a stock exchange as a dimension, the dimension name is "stock exchange" and the
            dimension value is "NYSE"
            When considering an AWS Region as a dimension, the dimension name is "region" and the dimension value
            is "us-east-1"
            For an IoT sensor, the dimension name is "device ID" and the dimension value is "12345"
    Measure - The actual value being measured by the record.
        Examples are the stock price, the CPU or memory utilization, and the temperature or humidity reading.
        Measures consist of measure names and measure values. Consider the following examples:
        For a stock price,
            the measure name is "stock price" and the measure value is the actual stock price at a point in time.
        For CPU utilization,
            the measure name is "CPU utilization" and the measure value is the actual CPU utilization.
    Timestamp - Indicates when a measure was collected for a given record.
        Timestream supports timestamps with nanosecond granularity.
    Table - A container for a set of related time series.
    Database - A top level container for tables.


    CSV Formats:
    Timestamps are in Unix time (UTC) and are in milliseconds.
    temp -
                 Unix Timestamp (UTC)  Degrees (°C)
        0               1630454400845        28.749
        1               1630454401845        28.749
        2               1630454402845        28.749

    eda -
                     Unix Timestamp (UTC)  EDA (microS)
        0               1630454400026      1.457198
        1               1630454400276      1.456375
        2               1630454400526      1.455566

    acc -
                  Unix Timestamp (UTC)     x (g)     y (g)     z (g)
        0                1630454400026  0.639414 -0.035401  0.631113
        1                1630454400057  0.673594  0.008545  0.682872
        2                1630454400089  0.649668 -0.063722  0.691173

    """

    def __init__(self, client):
        self.client = client

    def get_common_attrs(self, file_path, participant_id, device_id):
        dimensions = [
            {'Name': 'ppt_id', 'Value': participant_id},
            {'Name': 'device_id', 'Value': device_id}
        ]

        measure_name = None
        if "eda.csv" in file_path:
            measure_name = "eda_microS"
        elif "temp.csv" in file_path:
            measure_name = "temp_degC"
        elif "acc.csv" in file_path:
            measure_name = "acc_g"
        assert measure_name is not None

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'MeasureName': measure_name,
        }
        return common_attributes

    def get_timestream_df(self, file_path):
        # reformat CSV to Records series
        if "acc.csv" in file_path:
            # check if acc_reformatted.csv exists in the same directory
            # if it doesn't exist, create it

            df = pd.read_csv(file_path)
            df.rename(columns={df.columns[0]: "Time", df.columns[1]: "x", df.columns[2]: "y", df.columns[3]: "z"},
                      inplace=True)
            return df
        df = pd.read_csv(file_path)
        df.rename(columns={df.columns[0]: "Time", df.columns[1]: "MeasureValue"}, inplace=True)
        return df

    def write_records_with_common_attributes(self, participant_id, device_id, file_path):
        print(f"Writing records and extracting common attributes for {participant_id}...")
        common_attributes = self.get_common_attrs(file_path, participant_id, device_id)
        # reformat CSV to Records series
        df = self.get_timestream_df(file_path)

        batch_size = self.get_optimal_batch_size(df, common_attributes)

        num_batches = int(df.shape[0] // batch_size + 1)
        print(f"Writing {num_batches} batches of {batch_size} records each...")
        for i in range(num_batches):
            start = i * batch_size
            end = start + batch_size
            # returns a list of dicts...[{Time: val1, MeasureValue: val2}, ...]
            """
            unbatched temp per record
                time: 8 bytes
                dim1: 11 bytes ('ppt_id' + 'fc155')
                dim2: 16 bytes ('dev_id' + 'ABCDE12345')
                measure_name: 9 bytes ('temp_degC')
                                        eda_microS
                measure_value: 8 bytes
                    total: 52 bytes / record
                1 write = 1kb//52b = 19 records 
                    
            batched temp per record
                1kb - common: 11 + 16 + 9 = 36 bytes
                
                time + measure_value: 16 bytes
                    total: 16 bytes / record + common
                    1 write = (1kb-36)//16b = 60 records
                    
                
                
            
            
            """
            records = df[start:end].to_dict(orient='records')

            if i < num_batches - 1:
                # sanity check batches are the right size except for final batch which may be smaller
                assert batch_size >= len(records) > 0.9 * batch_size
            self.submit_batch(records, common_attributes, i + 1)

            # self.client.write_records(DatabaseName=DATABASE_NAME,
            #                           TableName=TABLE_NAME,
            #                           Records=records,
            #                           CommonAttributes=common_attributes)

    def submit_batch(self, records, common_attributes, counter):
        try:
            result = self.client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                               Records=records, CommonAttributes=common_attributes)
            print("Processed batch [%d]. WriteRecords Status: [%s]" % (counter,
                                                                       result['ResponseMetadata']['HTTPStatusCode']))
        except Exception as err:
            print("Error:", err)

    def get_optimal_writes_per_request(self, file_path):
        """
        100 records is the limit per request, figure out how many writes per request
        A timeseries write can be up to 1KB.
        Common attributes only need to be counted once so the number of events that can be fit in a 1KB write is

        (1000 - common_attr_size) // individual_event_size

        - We are charged for writes, so we want to minimize the number of writes per request.
        - We can fit at most 100 records per request
        - The number of writes is rounded to the nearest KB
        theoretical best case scenario is that we can fit 100 records into a single write
            this would require each record including common attributes to be 10 bytes
            (almost impossible, time alone is 8 bytes)


        imagine records are 50 bytes each, with 30 bytes of common attributes
        worst case scenario is sending one record at a time, so 100 records = 100 requests = 100 writes (charged 1kb ea)
        better case scenario is that we fit as many records as we can into a single write,
            so 1000 bytes / 50 bytes = 20 records per write (5x cheaper than worst case)
        best case scenario is that we refactor out the common attributes and fit as many records as we can into a single
        request.
            so (1000 bytes - 30 bytes) // 20 bytes = 48 records + common per write -> 2 writes per 100-record request
            ergo 50x cheaper than worst case

        sizes:
                dim1: 11 bytes ('ppt_id' + 'fc155')
                dim2: 16 bytes ('dev_id' + 'ABCDE12345')
                measure_name:
                    'acc_g': 5 bytes
                    'eda_microS': 10 bytes
                    'temp_degC': 9 bytes
                time: 8 bytes
                measure_value: 8 bytes (per measurement)
                    acc_g: 3x8 = 24 bytes
                    eda_microS: 8 bytes
                    temp_degC: 8 bytes

        """

        path_list = file_path.split(os.sep)
        assert len(path_list[-2]) == 10
        participant_id = path_list[-4].lower() + path_list[-3]
        assert len(participant_id) == 5

        csv_type = "acc" if "acc.csv" in file_path else "eda" if "eda.csv" in file_path else "temp"

        write_max = 1000
        params_by_type = {
            "temp": {
                # dim1 + dim2 + measure_name = 11 + 16 + 9 = 36 bytes
                "common_attr_size": 36,
                # time (8) + measure_value (8) = 16 bytes
                "record_size": 16,
            },
            "eda": {
                # dim1 + dim2 + measure_name = 11 + 16 + 10 = 37 bytes
                "common_attr_size": 37,
                # time (8) + measure_value (8) = 16 bytes
                "record_size": 16,
            },
            "acc": {
                # dim1 + dim2 + measure_name = 11 + 16 + 5 = 32 bytes
                "common_attr_size": 32,
                # time (8) + x_value (8) + y_value (8) + z_value (8) = 32 bytes
                "record_size": 32,
            },
        }
        common_attr_size = params_by_type[csv_type]["common_attr_size"]
        record_size = params_by_type[csv_type]["record_size"]
        max_records_per_request = 100
        max_records_per_write = (write_max - common_attr_size) // record_size
        writes_per_request = round(max_records_per_request / max_records_per_write)

        return writes_per_request

    def estimate_csv_write_cost(self, file_path):

        df_len = self.get_csv_len(file_path)

        # how many writes are needed for each 100-record request
        writes_per_request = self.get_optimal_writes_per_request(file_path)

        num_requests_per_df = df_len // 100 + 1
        num_writes_in_df = num_requests_per_df * writes_per_request

        cost = num_writes_in_df * (0.50 / 1000000)

        # price is $0.50 / 1M writes
        print(f"Estimated cost for {file_path}: ${cost}")
        return cost

    def get_csv_len(self, file_path):
        p = subprocess.Popen(['wc', '-l', file_path], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        result, err = p.communicate()
        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0]) + 1
