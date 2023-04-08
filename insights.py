import pandas as pd
from calendar import monthrange

def wear_time(file_path):
    """Calculate the percentage of time each participant wore the watch.
    Parameters: file_path (str): The file path of the data to be read.
    Returns: A pandas dataframe containing the percentage of time each participant wore the watch.
    Example: wear_time("data/example.csv")
    """
    df = pd.read_csv(file_path)
    EDA_filter = 0.03
    df['Time'] = pd.to_datetime(df['Time'], unit="ms")
    month = df['Time'].dt.month[0]
    year = df['Time'].dt.year[0]
    days_in_month = monthrange(year, month)[1]
    # 24/day but needs 1.75 hours of charge for every 24 of wear
    hours_wearable = 24 - 1.75
    # seconds in a month
    seconds_in_month = 60 * 60 * hours_wearable * days_in_month
    # EDA is measured 4x per second
    metrics_possible_per_month = seconds_in_month * 4

    # breakpoint()

    return round((df.loc[(df['MeasureValue'] >= EDA_filter)].groupby('ppt_id').count().Time * 100) / (metrics_possible_per_month), 2)
