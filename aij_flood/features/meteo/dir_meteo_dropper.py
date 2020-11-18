from . import preproc_utils


class DirMeteoDropper:
    def __init__(self):
        self.datetime_cols = ["year", "month", "day", "time", "localYear", "localMonth", "localDay",
                              "localTimePeriod", "timePeriodNum", "localTime", "tz", "startMeteoDay"]
        self.drop_substrings = ["Sign", "Quality"]
        self.cols_nan_threshold = 0.2
        self.unable_forecast_cols = ["pastWeather", "presentWeather", "maximumWindGustSpeed",
                               "characteristicOfPressureTendency", "HourPressureChange3", 'vapourPressure']

    def drop_cols(self, df):
        cols = df.columns

        drop_cols = []
        drop_cols += self.datetime_cols
        drop_cols += self.unable_forecast_cols
        drop_cols += preproc_utils.filter_substring(cols, "Sign")
        drop_cols += preproc_utils.filter_substring(cols, "Quality")
        drop_cols += preproc_utils.find_cols_nan_threshold(df, 0.2)

        return df.drop(columns=drop_cols)