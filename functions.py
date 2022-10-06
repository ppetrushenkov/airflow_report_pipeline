from numpy import ndarray

from config import CONNECTION, CUR_DIR
from typing import Tuple

import io
import numpy as np
import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt


def get_query(path_to_query: str) -> str:
    """Return the query as string from the certain path"""
    full_path = CUR_DIR + '/sql/' + path_to_query
    with open(full_path, "r") as f:
        query = f.read()
    return query


def get_data(path_to_query: str) -> pd.DataFrame:
    """Return DataFrame for specified query"""
    query = get_query(path_to_query=path_to_query)
    return ph.read_clickhouse(query, connection=CONNECTION)


def get_avg_info(info: pd.DataFrame):
    """Return data for yesterday, before yesterday and avg values for last week"""
    last_two_days = info.iloc[-2:, :]
    last_week = info.iloc[-12:-6, :]
    last_week = last_week.mean(axis=0)

    last_week_df = pd.DataFrame(last_week).T
    last_week_df.index = ['avg last week']
    return pd.concat([last_week_df, last_two_days], axis=0).round(2)


def join_by_groups(data: pd.DataFrame) -> pd.DataFrame:
    """Groups the data by the "status" column and then combines them into one DataFrame"""
    dfs = [x for _, x in data.groupby('status')]

    df = pd.DataFrame()
    df.index = dfs[0].index
    df.sort_index(inplace=True)

    df['gone'] = dfs[0]['num_users']
    df['new'] = dfs[1]['num_users']
    df['retained'] = dfs[2]['num_users']
    return df[['retained', 'new', 'gone']]


def get_table_pic(info: pd.DataFrame, info_type: str) -> io.BytesIO:
    """Return table as picture object"""
    N = info.shape[0]

    fig, ax1 = plt.subplots(figsize=(10, 2 + N / 2.5))
    table = ax1.table(
        cellText=info.values,
        cellLoc='center',
        rowLoc='center',
        colLabels=info.columns,
        colColours=['lavender'] * info.shape[1],
        loc='center'
    )
    table.scale(1, 2)
    table.auto_set_font_size(False)
    table.set_fontsize(13)
    ax1.axis('off')
    title = f"Short summary [{info_type}]"
    ax1.set_title(title, weight='bold', size=16, color='k')

    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=350, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'short_info.png'
    plt.close()
    return plot_object


def get_previous_dates(metric: pd.Series) -> pd.Series:
    """Return data, where time column is the same 
    as the last time in our data. For example if the last time 
    in our metric is 15:45, we filter out rows with time 15:45 only.
    """
    df = metric.to_frame()
    df.reset_index(inplace=True)

    df['Dates'] = pd.to_datetime(df['15time']).dt.date
    df['Time'] = pd.to_datetime(df['15time']).dt.time
    df.drop('15time', axis=1, inplace=True)
    df.set_index("Dates", inplace=True)

    last_time = df['Time'].values[-1]
    return df[df['Time'] == last_time].iloc[:, 0].squeeze()


def get_iqr(metric: pd.Series) -> Tuple[float, float, float]:
    """
    Return Interquartile Range (IQR) and (25, 75) quantile of data.

    Info:
    -----
    Calculate the 25 and 75 quantile of data and the range between them (iqr).

    Return:
        Tuple(
            IQR: float,
            First quantile: float
            Third quantile: float
        )
    """
    quantile1 = metric.quantile(0.25)
    quantile3 = metric.quantile(0.75)
    iqr = quantile3 - quantile1

    return iqr, quantile1, quantile3


def zscale(metric: pd.Series) -> pd.Series:
    """
    Return Z scaled metric, using Standard Scaler method.
    Z score shows, how far the value is from the mean in Standard Deviation scale.
    Formula: z = (x - mean) / std
    """
    z = (metric - metric.mean()) / metric.std()
    return z


def median_abs_dev(metric: pd.Series) -> ndarray:
    """Return Median Absolute Deviation"""
    median = metric.median()
    dev = np.abs(metric - median)
    return np.median(dev)


def modified_zscale(metric: pd.Series) -> pd.Series:
    """
    Use modified Z Score transform.

    Formula:
    --------
    modZ = 0.6745 * (x - median) / MAD
        where: MAD is median absolute deviation

    Info:
    -----
    As the simple Z score, modified Z score shows, how far the value from the mean (median here)
    in standard deviation values. Modified Z score use median instead of mean, because the mean 
    is too sensitive to outliers.
    """
    median = metric.median()
    mad = median_abs_dev(metric)
    z = (0.6745 * (metric - median)) / mad
    return z


def get_pct_change(a: float, b: float) -> float:
    "Return percentage difference between two numbers"
    return 100 * abs(a - b) / ((a + b) / 2)


def get_bollinger_bands(metric: pd.Series, window: int = 21, coef: int = 4, usemedian: bool = True):
    """
    Return Bollinger bands indicator for specified series data.

    Info:
    ------
    Bollinger bands consists of the Simple Moving Average (SMA), Upper band and Lower band.
    SMA is just the mean of data with specified window. Here we use median instead of mean.
    Upper/Lower band is the mean value plus/minus Standard Deviation value 
    multiplied by some coefficient (usually 2, here 4 just for anomalies).
    Because of anomalies, instead of STD we use MAD (Median Absolute Deviation).
 
    Upper band = SMA + STD * coef.
    Lower band = SMA - STD * coef.

    STD: Standard deviation

    Args:
        metric (pd.Series): metric time series data
        window (int): the period, that data will be smoothed

    Returns:
        sma: Simple Moving Average
        upper_band: Upper Bollinger band
        lower_band: Lower Bollinger band
    """
    if usemedian:
        sma = metric.rolling(window).median()
        std = metric.rolling(window).apply(median_abs_dev)

    else:
        sma = metric.rolling(window).mean()
        std = metric.rolling(window).std()

    upper_band = sma + std * coef
    lower_band = sma - std * coef

    return sma, upper_band, lower_band
