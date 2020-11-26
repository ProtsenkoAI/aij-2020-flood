# TODO: refactor
# TODO: add namer in manager, delete it here

from collections.abc import Iterable
import pandas as pd
import numpy as np
from .. import utils


def station_features(df, func, func_args_list):
    id_groups = df.groupby("id")

    features = []

    for func_args in func_args_list:
        # print(func_args)
        if not isinstance(func_args, Iterable):
            func_args = (func_args,)
        func_out = func(id_groups, *func_args)

        features.append(func_out)

    features_df = pd.concat(features, axis=1)
    return features_df


def lag(grouped, lag):
    feature = grouped.shift(lag)

    feature_name = f"lag_{lag}"
    # feature = utils._rename_first_col(feature, feature_name)
    feature.name = feature_name

    return feature


def diff_lag(grouped, ndays):
    differences = grouped.diff(1).groupby("id").shift(1)
    diff_index = differences.index
    differences = differences.groupby("id")  # group again to roll by groups not whole df

    feature_names = [f"diff_{i}" for i in range(1, ndays + 1)]
    diff_lags = differences.rolling(ndays, min_periods=1)  # need shift to get only previous values without current one

    diff_lags = _all_rolling_lags(diff_lags, ndays)

    diff_lags_df = pd.DataFrame(diff_lags, columns=feature_names, index=diff_index)
    return diff_lags_df


def _all_rolling_lags(roll, ndays):
    vals = []
    for window in enumerate(roll):
        window_vals_flipped = window[1].values[:, 0]
        # window is took from earlier days to later, so we flip it to lag format (1 col is lag 1 day ago etc)
        window_vals = np.flip(window_vals_flipped)

        win_len = window_vals.shape[0]

        if win_len < ndays:
            padding_array = np.full(ndays - win_len, np.nan)
            padded_window_values = np.concatenate((window_vals, padding_array))

        else:
            padded_window_values = window_vals

        vals.append(padded_window_values)
    return np.array(vals)

