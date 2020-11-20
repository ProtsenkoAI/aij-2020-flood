# TODO: refactor

import numpy as np
from . import preproc_utils
from . import extr_utils


class DirMeteoPreprocessor:
    def __init__(self, df, dropper, dt_builder, diff_cols):
        self.df = df

        self.dt_builder = dt_builder
        self.dropper = dropper
        self.diff_cols = diff_cols

        self.dt_colname = "datetime"

    def preprocess(self): # rename for "run"
        self.build_datetime()
        self.drop_cols()
        self.process_columns()
        self.add_diff()
        return self.df

    def build_datetime(self):
        dt_vals = self.dt_builder.build(self.df)
        self.df[self.dt_colname] = dt_vals

    def drop_cols(self):
        new_df = self.dropper.drop_cols(self.df)
        self.df = new_df

    def process_columns(self):
        self._scale_cloud_cover()
        self._wind_angle_to_x_y()

    def _scale_cloud_cover(self):
        col = self.df["cloudCoverTotal"]
        col[col == 12] = 9.5  # согласно README, это "10" с просветами
        col[col == 11] = 0.05  # следы облаков
        col[col == 13] = np.nan  # облака невозможно определить
        self.df["cloudCoverTotal"] = col

    def _wind_angle_to_x_y(self):
        wind_angle_x, wind_value_y = preproc_utils.angle_to_x_y(self.df["windDirection"])
        self.df["windAngleX"] = wind_angle_x
        self.df["windAngleY"] = wind_value_y

        self.df.drop(columns="windDirection", inplace=True)

    def add_diff(self):
        self.df.set_index(["stationNumber", self.dt_colname], inplace=True)
        diff_cols_grouped = self.df[self.diff_cols].groupby("stationNumber")
        diff_values = diff_cols_grouped.diff()
        diff_values.columns = extr_utils.plural_create_colname(diff_values, "diff")
        self.add_features(diff_values)

    def add_features(self, features):
        self.df = self.df.merge(features, left_index=True, right_index=True)


class ForecastMeteoPreprocessor:
    def __init__(self):
        self.cloud_cover_col = "cloudCoverTotal"
        self.x_wind_col = 'windAngleX'
        self.y_wind_col = 'windAngleY'

        self.temperature_cols = ["airTemperature", "soilTemperature",
                                 "maximumTemperatureOverPeriodSpecified",
                                 "minimumTemperatureAtHeightAndOverPeriodSpecified",
                                 "dewpointTemperature"
                                 ]

        self.pressure_cols = ["pressure", "pressureReducedToMeanSeaLevel"]

    def preprocess(self, forecast_df):
        rescaled_df = self.rescale(forecast_df)
        return rescaled_df

    def rescale(self, df):
        df = self._rescale_cloud_cover(df)
        df = self._rescale_wind_vector(df)
        df = self._rescale_temperature(df)
        df = self._rescale_pressure(df)
        return df

    def _rescale_cloud_cover(self, df):
        df[self.cloud_cover_col] /= 10  # rescaling from 0-100 to 0-10
        return df

    def _rescale_wind_vector(self, df):
        wind_x = df[self.x_wind_col]
        wind_y = df[self.y_wind_col]
        vector_module = (wind_x ** 2 + wind_y ** 2) ** 0.5

        wind_x = np.sign(wind_x) * wind_x / vector_module
        wind_y = np.sign(wind_y) * wind_y / vector_module

        # if vector_module == 0, wind_x and wind_y are np.nan
        wind_x[vector_module == 0] = 0
        wind_y[vector_module == 0] = 0

        df[self.x_wind_col] = wind_x
        df[self.y_wind_col] = wind_y

        return df

    def _rescale_temperature(self, df):
        df[self.temperature_cols] -= 273  # temperature there is in absolute form
        return df

    def _rescale_pressure(self, df):
        df[self.pressure_cols] /= 100
        return df