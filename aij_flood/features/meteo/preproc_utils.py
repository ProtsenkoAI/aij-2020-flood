import numpy as np


def filter_substring(strings, substring):
    def _check_substring(string):
        return string.find(substring) != -1  # returned when wasn't found

    filtered_strings = list(filter(_check_substring, strings))
    return filtered_strings


def find_cols_nan_threshold(df, nan_freq_threshold=0.2):
    cols_nan_count = df.isna().sum(axis=0)
    enough_nan_mask = cols_nan_count >= (len(df) * nan_freq_threshold)
    nan_cols = list(df.columns[enough_nan_mask])

    return nan_cols


def angle_to_x_y(angles):
    out_x, out_y = np.zeros(len(angles)), np.zeros(len(angles))

    not_null_mask = (angles != 0) & (angles != 999)

    # working only with values in range (1, 360]
    angles = angles[not_null_mask]

    # from classical wind angles to geometry angles
    right_coords_angles = 90 - angles
    right_coords_angles[right_coords_angles < 0] += 360

    radians = np.radians(right_coords_angles)
    coses, sines = np.cos(radians), np.sin(radians)

    out_x[not_null_mask] = coses
    out_y[not_null_mask] = sines

    return out_x, out_y