def filter_cols_min_nan_freq(df, min_nan_freq):
    cols_nan_count = df.isna().sum(axis=0)
    enough_nan_mask = cols_nan_count >= (len(df) * min_nan_freq)
    nan_cols = list(df.columns[enough_nan_mask])
    
    return nan_cols


def filter_rows_min_nan_count(df, min_nan_count):
    rows_nan_count = df.isna().sum(axis=1)
    enough_nan_mask = rows_nan_count >= min_nan_count
    nan_rows = list(df.index[enough_nan_mask])
    
    return nan_rows