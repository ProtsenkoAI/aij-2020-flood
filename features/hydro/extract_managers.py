from extractor import Extractor
import pandas as pd


class ExtractManager:
    def __init__(self, water_levels: pd.DataFrame, hydro_coords: pd.DataFrame, extractor: Extractor):
        self.water_levels = water_levels
        self.hydro_coords = hydro_coords
        self.extractor = extractor

    def extract(self):
        df = self.water_levels
        levels_stats = self.extractor.levels_stats(df)
        lags = self.extractor.lags(df)
        diff_features = self.extractor.diff_features(df)
        doy_stats = self.extractor.doy_stats(df)

        extracted_features = [levels_stats, lags, diff_features, doy_stats]
        all_features = self._unite_features(extracted_features)
        return all_features

    def _unite_features(self, df_list):
        if self._check_same_idxs(df_list):
            return pd.concat(df_list, axis=1)
        else:
            raise ValueError("dfs have different indexes, cant concatenate")

    def _check_same_idxs(self, dfs):
        first_df = dfs[0]
        for df in dfs[1:]:
            if not first_df.index.equals(df.index):
                return False
        return True


class LastDayExtractManager(ExtractManager):
    def __init__(self, water_levels, hydro_coords, extractor, days_usage_config):
        """
        check ExtractManager docs
        :param days_usage_config: mapping with keys ("lags", "diff", "levels_stat", "doy")
        and values containing int. Values'll be used to filter water_levels by last *value* days
        before extracting features to optimize process.
        """
        super().__init__(water_levels, hydro_coords, extractor)
        self.days_usage_config = days_usage_config

    def extract(self):
        levels_stats = self._levels_stats()
        lags = self._lags()
        diff_features = self._diff_features()
        doy_stats = self._doy_stats

        all_features = self._unite_features([levels_stats, lags, diff_features, doy_stats])
        return all_features

    def get_filtered_levels(self, usage_config_key):
        days_needed = self.days_usage_config[usage_config_key]
        filtered_levels = self.filter_water_latest_n_days(days_needed)
        return filtered_levels

    def filter_water_latest_n_days(self, n):
        dates = self.water_levels.reset_index()["date"]
        last_n_uniq_dates = sorted(dates.unique())[-n:]

        mask_last_n_dates = dates.isin(last_n_uniq_dates).values
        return self.water_levels[mask_last_n_dates]

    def _levels_stats(self):
        stats_levels = self.get_filtered_levels("levels_stat")
        return self.extractor.levels_stats(stats_levels)

    def _lags(self):
        lags_levels = self.get_filtered_levels("lags")
        return self.extractor.lags(lags_levels)

    def _diff_features(self):
        diff_levels = self.get_filtered_levels("diff")
        return self.extractor.diff_features(diff_levels)

    def _doy_stats(self):
        doy_levels = self.get_filtered_levels("doy")
        return self.extractor.doy_stats(doy_levels)
