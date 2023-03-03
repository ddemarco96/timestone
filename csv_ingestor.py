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
<<<<<<< Updated upstream
=======
        elif "acc.csv" in file_path:
            measure_name = "acc_g"
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
    def write_records_with_common_attributes(self, participant_id, device_id, filepath):
        print(f"Writing records and extracting common attributes for {participant_id}...")
        common_attributes = self.get_common_attrs(filepath, participant_id, device_id)
        # reformat CSV to Records series
        df = self.get_timestream_df(filepath)
=======
        with pd.read_csv(file_path, header=0, names=names, chunksize=chunksize, dtype=dtypes) as reader:
            start_time = time.time()
            for chunk in reader:  # each chunk is a df
                chunks_read += 1
                records_read += chunk.shape[0]
                if verbose:
                    print(f"Processing chunk {chunks_read} with {chunk.shape[0]} records...")
                # Add dimensions to chunk
>>>>>>> Stashed changes

        batch_size = self.get_optimal_batch_size(df, common_attributes)

<<<<<<< Updated upstream
        num_batches = int(df.shape[0] // batch_size + 1)
        print(f"Writing {num_batches} batches of {batch_size} records each...")
        for i in range(num_batches):
            start = i * batch_size
            end = start + batch_size
            # returns a list of dicts...[{Time: val1, MeasureValue: val2}, ...]
            records = df[start:end].to_dict(orient='records')

            if i < num_batches - 1:
                # sanity check batches are the right size except for final batch which may be smaller
                assert batch_size >= len(records) > 0.9 * batch_size
            self.submit_batch(records, common_attributes, i + 1)
=======
                if verbose:
                    end_time = time.time()
                    print("Chunk read complete. Took {} seconds".format(end_time - start_time))
        return records_read
>>>>>>> Stashed changes

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

    def get_optimal_batch_size(self, df, common_attributes, verbose=False):
        """
        A timeseries write can be up to 1KB.
        Common attributes only need to be counted once so the number of events that can be fit in a 1KB write is

        (1000 - common_attr_size) // individual_event_size
        """
<<<<<<< Updated upstream
=======
        csv_type = "acc" if "acc.csv" in file_path else "eda" if "eda.csv" in file_path else "temp"

>>>>>>> Stashed changes
        write_max = 1000
        common_attr_size = getsizeof(common_attributes)
        avg_row_size = round(df.memory_usage(deep=True).sum() / df.shape[0])
        batch_size = (write_max - common_attr_size) // avg_row_size

        if verbose:
            print(f"Average row size: {avg_row_size} bytes")
            print(f"Num of records per write: {batch_size}")
        return batch_size

<<<<<<< Updated upstream
    def estimate_csv_write_cost(self, filepath, participant_id, device_id):
        common_attributes = self.get_common_attrs(filepath, participant_id, device_id)

        # reformat CSV to Records series
        df = self.get_timestream_df(filepath)
        batch_size = self.get_optimal_batch_size(df, common_attributes)

        num_writes_in_df = df.shape[0] // batch_size + 1
=======
    def estimate_csv_write_cost(self, file_path, df_rows, verbose=False):
        # how many writes are needed for each 100-record request
        writes_per_request = self.get_optimal_writes_per_request(file_path)

        num_requests_per_df = df_rows // 100 + 1
        num_writes_in_df = num_requests_per_df * writes_per_request
>>>>>>> Stashed changes

        cost = num_writes_in_df * (0.50 / 1000000)

        # price is $0.50 / 1M writes
<<<<<<< Updated upstream
        print(f"Estimated cost for {filepath}: ${cost}")
        return cost
=======
        if verbose:
            print(f"Estimated cost for {file_path}: ${cost}")
        return cost

    @staticmethod
    def estimate_csv_write_time(file_path, df_rows, verbose=False):
        # 1M records takes about 11 minutes to write
        minutes = round(df_rows * (11 / 1000000), 2)
        if verbose:
            print(f"Estimated time for {file_path}: {minutes} minutes")
        return minutes

    @staticmethod
    def get_num_rows(file_path):
        # the number of lines in the csv is the number of records + 1 (header)
        p = subprocess.Popen(['wc', '-l', file_path], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        result, err = p.communicate()
        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0]) - 1

    def list_databases(self):
        print("Listing databases")
        try:
            result = self.client.list_databases(MaxResults=5)
            self._print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_databases(NextToken=next_token, MaxResults=5)
                self._print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

    @staticmethod
    def _print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])
>>>>>>> Stashed changes
