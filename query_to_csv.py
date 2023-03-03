import pandas as pd


class CSVQuery:
    """
    This class runs a query and converts the results directly to a CSV. It can also run a list containing
    multiple queries

    Example of an input query string given DATABASE_NAME, TABLE_NAME, and HOSTNAME constants:

        SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp,
            ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization,
            ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization,
            ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization,
            ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization
        FROM {DATABASE_NAME}.{TABLE_NAME}
        WHERE measure_name = 'cpu_utilization'
        AND hostname = '{HOSTNAME}'
        AND time > ago(2h)
        GROUP BY region, hostname, az, BIN(time, 15s)
        ORDER BY binned_timestamp ASC

    """

    def __init__(self, client):
        self.client = client
        self.paginator = client.get_paginator('query')

    # run a list of queries
    def run_all_queries(self, queries, loc):
        for query_id in range(len(queries)):
            print("Running query [%d] : [%s]" % (query_id + 1, queries[query_id]))
            self.run_query(queries[query_id], loc)

    # run a single query
    def run_query(self, query_string, loc):
        try:
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                self._parse_query_result(page, loc)
        except Exception as err:
            print("Exception while running query:", err)

    def _parse_query_result(self, query_result, loc):
        # set up columns
        columns = []
        column_info = query_result['ColumnInfo']
        for j in range(len(column_info)):
            columns.append(column_info[j]['Name'])

        # set up rows
        rows = []
        for row in query_result['Rows']:
            row_list = []
            data = row['Data']
            for j in range(len(data)):
                row_list.append(list(data[j].values())[0])
            rows.append(row_list)

        # uncomment to display output
        # print("Columns:")
        # print(columns)
        # print("Rows:")
        # print(rows)

        # create and save df from columns and rows
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(loc)