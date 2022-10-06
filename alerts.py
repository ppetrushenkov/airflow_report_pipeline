from functions import *
import pandas as pd


def previous_days_alert(metric: pd.Series, coef: int = 2) -> bool:
    """
    If current point deviates too much from the points at the same time
    in previous days, this may indicate the presence of an outlier.

    Info:
    -----
    Filter out only those rows, whose time is the same as the last time value in metric.
    Roughly speaking, if current time is 15:45, we filter out all rows with the time at 15:45.
    """
    prev_metric = get_previous_dates(metric)
    std = prev_metric.std()
    mean = prev_metric.mean()
    upper_bound = mean + coef * std
    lower_bound = mean - coef * std
    last_value = prev_metric[-1]

    if (last_value > upper_bound) or (last_value < lower_bound):
        return True
    return False


def iqr_alert(metric: pd.Series) -> bool:
    """
    Check data, if it goes too far from the Interquartile Range of series
    
    Info:
    -----
    If our value is greater/lower than the 75/25 quantile of data multiplied by 1.5 of interquartiled range,
    this means, that our last value too far from the median, so we can assume that we have an outlier.
    """
    iqr, q1, q3 = get_iqr(metric)
    last_value = metric[-1]

    high_outlier = (last_value > q3 + 1.5 * iqr)
    low_outlier = (last_value < q1 - 1.5 * iqr)
    if high_outlier or low_outlier:
        return True
    return False


def zscore_alert(metric: pd.Series, dev: int = 3, modified: bool = True) -> bool:
    """
    Transform data using Z score technique, then if the last value greater
    specified value (dev here), it is possible we have an outlier.
    """
    if modified:
        z_metric = modified_zscale(metric=metric)
    else:
        z_metric = zscale(metric=metric)

    last_value = z_metric[-1]
    if (last_value > dev) or (last_value < -dev):
        return True
    return False


def bollinger_bands_alert(metric: pd.Series, window: int = 21, coef: int = 5) -> bool:
    """
    Get bollinger bands
    If some feature value greater or lower some band -> return True (ALERT!)
    """
    last_value = metric[-1]
    sma, upper_band, lower_band = get_bollinger_bands(metric, window, coef)
    last_ub, last_lb = upper_band.values[-1], lower_band.values[-1]
    if (last_value > last_ub) or (last_value < last_lb):
        return True
    return False
