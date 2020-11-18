import pandas as pd


class DtBuilder:
    def __init__(self):
        self.dt_colname2pandas_names = {"localYear": "year", "localMonth": "month", "localDay": "day"}
        self.dt_colnames = self.dt_colname2pandas_names.keys()
        self.hours_col = "localTime"

    def build(self, df):
        date = self._create_dt_from_cols(df)
        time = pd.to_timedelta(df[self.hours_col], unit="hour")
        datetime_col = date + time

        return datetime_col

    def _create_dt_from_cols(self, df):
        date_columns = df[self.dt_colnames]
        date_columns.rename(columns=self.dt_colname2pandas_names, inplace=True)
        # print(date_columns)
        return pd.to_datetime(date_columns)
