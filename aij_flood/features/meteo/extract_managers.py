from . import extr_utils
from .. import utils
import numpy as np
import pandas as pd
# TODO: мб валидировать входящие конфиги на корректность


class MeteoExtractManager:
    def __init__(self, config_builder):
        self.id_col = "stationNumber"
        self.config_builder = config_builder

    def extract(self, meteo):
        meteo = self.agg_daily(meteo)
        meteo = self.fill_missing_dates(meteo)
        features = self.stats(meteo)
        return features

    def agg_daily(self, meteo):
        grouped = extr_utils.groupby_station_date(meteo)
        daily_mean = grouped.agg(np.nanmean)
        daily_mean.reset_index().set_index([self.id_col, "date"])

        return daily_mean

    def stats(self, meteo):
        config = self.config_builder.build(meteo)

        stats = []
        for colname, aggs_list in config.items():
            col_stats = self._col_aggs(meteo, colname, aggs_list)
            stats += col_stats

        stats = pd.concat(stats, axis=1)

        return self.add_features(meteo, stats)

    def _col_aggs(self, meteo, colname, aggs_list):
        col_vals = meteo[colname]
        agged_features = []

        for agg_params in aggs_list:
            func, lag, winsize = agg_params["func"], agg_params["lag"], agg_params["winsize"]
            agged = utils.roll_shift_agg(col_vals, func, lag, winsize, station_col_name=self.id_col)
            agged.name = extr_utils.create_colname(colname, func.__name__, lag, winsize)
            agged_features.append(agged)

        return agged_features

    def add_features(self, meteo, features):
        return meteo.merge(features, left_index=True, right_index=True)

    def fill_missing_dates(self, df):
        dates = df.reset_index()["date"]
        ids = df.index.get_level_values("stationNumber").unique()
        new_date_index = pd.date_range(dates.min(), dates.max(), name="date")

        all_dates_index = pd.MultiIndex.from_product([ids, new_date_index])

        return df.reindex(all_dates_index)
