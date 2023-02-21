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

    def get_common_attrs(self, filepath, participant_id, device_id):
        dimensions = [
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
        return common_attributes

    def get_timestream_df(self, filepath):
        # reformat CSV to Records series
        df = pd.read_csv(filepath)
        df.rename(columns={df.columns[0]: "Time", df.columns[1]: "MeasureValue"}, inplace=True)
        return df

    def write_records_with_common_attributes(self, participant_id, device_id, filepath):
        print(f"Writing records and extracting common attributes for {participant_id}...")
        common_attributes = self.get_common_attrs(filepath)

        # reformat CSV to Records series
        df = self.get_timestream_df(filepath)




        records = []
        for index, row in df.iterrows():
            record = {
                'MeasureValue': str(row[1]),
                'Time': str(self._current_milli_time()),
                'TimeUnit': 'MILLISECONDS',
            }
            records.append(record)

    def get_optimal_batch_size(self, df, common_attributes, verbose=False):
        """
        A timeseries write can be up to 1KB.
        Common attributes only need to be counted once so the number of events that can be fit in a 1KB write is

        (1000 - common_attr_size) // individual_event_size
        """
        write_max = 1000
        common_attr_size = getsizeof(common_attributes)
        avg_row_size = df.memory_usage(deep=True).sum() / df.shape[0]
        batch_size = (write_max - common_attr_size) // avg_row_size

        if verbose:
            print(f"Average row size: {avg_row_size} bytes")
            print(f"Num of records per write: {batch_size}")
        return batch_size

    def estimate_csv_write_cost(self, filepath, participant_id, device_id):
        common_attributes = self.get_common_attrs(filepath, participant_id, device_id)

        # reformat CSV to Records series
        df = self.get_timestream_df(filepath)
        batch_size = self.get_optimal_batch_size(df, common_attributes)

        num_writes_in_df = df.shape[0] // batch_size + 1

        cost = num_writes_in_df * (0.50 / 1000000)

        # price is $0.50 / 1M writes
        print(f"Estimated cost for {filepath}: ${cost}")
        return cost
