from . import extr_utils
from .. import utils
import numpy as np
import pandas as pd
# TODO: мб валидировать входящие конфиги на корректность


class MeteoExtractManager:
    def __init__(self, preprocessed_meteo, config):
        self.meteo = preprocessed_meteo
        self.id_col = "stationNumber"
        self.config = config

    def agg_daily(self):
        grouped = extr_utils.groupby_station_date(self.meteo)
        daily_mean = grouped.agg(np.nanmean)
        daily_mean.reset_index().set_index([self.id_col, "date"])

        self.meteo = daily_mean

    def stats(self):
        stats = []
        for col, aggs_list in self.config.items():
            col_stats = self._col_aggs(col, aggs_list)
            stats += col_stats

        stats = pd.concat(stats, axis=1)

        self.add_features(stats)

    def _col_aggs(self, col, aggs_list):
        agged_features = []

        for agg_params in aggs_list:
            func, lag, winsize = agg_params["func"], agg_params["lag"], agg_params["winsize"]
            col_vals = self.meteo[col]
            agged = utils.roll_shift_agg(col_vals, func, lag, winsize, station_col_name=self.id_col)
            agged.name = extr_utils.create_colname(col, func.__name__, lag, winsize)
            agged_features.append(agged)

        return agged_features

    def add_features(self, features):
        self.meteo = self.meteo.merge(features, left_index=True, right_index=True)

    def get_data(self):
        return self.meteo