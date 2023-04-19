"""Functions to estimate the cost and time of various operations"""

def walking_cost(path_rows, ingestor, verbose):
    """Calculate the total cost of writing CSV files for a given set of paths and row counts.

    This function takes in a list of tuples containing a path and a number of rows, an Ingestor object, and a boolean
    indicating whether verbose output should be printed. It calculates the total cost of writing CSV files by looping
    through the list of tuples and using the Ingestor object to estimate the cost of writing each file.

    Parameters:
        path_rows (list): A list of tuples containing paths and associated row counts
        ingestor (Ingestor): An Ingestor object used to estimate the cost of writing CSV files
        verbose (bool): A boolean indicating whether verbose output should be printed

    Returns:
        total_cost (int): The total cost of writing the CSV files

    Examples:
        ingestor = Ingestor()
        path_rows = [('/path/to/file1.csv', 1000), ('/path/to/file2.csv', 2000)]
        walking_cost(path_rows, ingestor, verbose=True)
        500
    """
    total_cost = 0
    for (path, num_rows) in path_rows:
        total_cost += ingestor.estimate_csv_write_cost(file_path=path, df_rows=num_rows, verbose=verbose)
    return total_cost

def walking_time(path_rows, ingestor, verbose):
    """Return the time in minutes it will take to upload all the files in path_rows"""
    total_time = 0
    for (path, num_rows) in path_rows:
        total_time += ingestor.estimate_csv_write_time(file_path=path, df_rows=num_rows, verbose=verbose)
    return total_time
