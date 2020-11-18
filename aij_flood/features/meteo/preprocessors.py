import numpy as np
from . import preproc_utils


class DirMeteoPreprocessor:
    def __init__(self, df, dropper, dt_builder):
        self.df = df

        self.dt_builder = dt_builder
        self.dropper = dropper

        self.dt_colname = "datetime"

    def preprocess(self): # rename for "run"
        self.build_datetime()
        self.drop_cols()
        self.process_columns()
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
        self.df ["cloudCoverTotal"] = col

    def _wind_angle_to_x_y(self):
        wind_angle_x, wind_value_y = preproc_utils.angle_to_x_y(self.df["windDirection"])
        self.df["windAngleX"] = wind_angle_x
        self.df["windAngleY"] = wind_value_y

        self.df.drop(columns="windDirection", inplace=True)
