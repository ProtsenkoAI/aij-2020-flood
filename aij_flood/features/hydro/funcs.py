# TODO: put all realization specifications to core_funcs.py
# TODO: refactor

import pandas as pd
from . import core_funcs
from .. import utils


def stats(df, config):
    return core_funcs.station_features(df, utils.roll_shift_agg, config)


def lags(df, lag_days):
    return core_funcs.station_features(df, core_funcs.lag, lag_days)


def diff_lag(df, lag_days):
    return core_funcs.station_features(df, core_funcs.diff_lag, lag_days)


def diff_stats(diff_lags_df, stat_funcs):
    # TODO: write clean realisation using just water levels, rolling() and agg()
    features = []
    for func in stat_funcs:
        f_name = f"diff_{func.__name__}"
        feature = diff_lags_df.apply(func, axis=1)
        feature.name = f_name

        features.append(feature)

    features_df = pd.concat(features, axis=1)

    return features_df


def past_years_stats(df, stat_funcs):
    months = df.index.get_level_values("date").month
    days = df.index.get_level_values("date").day
    station = df.index.get_level_values("id")

    same_doy = df["target"].groupby([months, days, station])
    same_doy_previous = same_doy.shift(1).groupby([months, days, station])
    expanded = same_doy_previous.expanding(min_periods=1)

    features = []
    for func in stat_funcs:
        print(f"agg func: {func.__name__}")
        # agg from previous because we have no water level of x year's doy when calc features for this year's doy
        feature = expanded.agg(func)
        f_name = f"doy_{func.__name__}"
        #         feature = rename_first_col(feature, f_name)
        feature.name = f_name
        features.append(feature)

    features_df = pd.concat(features, axis=1)
    features_df = features_df.droplevel([0, 1, 2]).sort_index(level=["id", "date"])

    return features_df
