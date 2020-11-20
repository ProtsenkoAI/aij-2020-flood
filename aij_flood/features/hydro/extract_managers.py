from .extractor import Extractor
import pandas as pd


class ExtractManager:
    def __init__(self, hydro_coords: pd.DataFrame, extractor: Extractor):
        self.hydro_coords = hydro_coords
        self.extractor = extractor

    def extract(self, water_levels):
        levels_stats = self.extractor.levels_stats(water_levels)
        lags = self.extractor.lags(water_levels)
        diff_features = self.extractor.diff_features(water_levels)
        doy_stats = self.extractor.doy_stats(water_levels)

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
    def __init__(self, hydro_coords, extractor, days_usage_config):
        """
        check ExtractManager docs
        :param days_usage_config: mapping with keys ("lags", "diff", "levels_stat", "doy")
        and values containing int. Values'll be used to filter water_levels by last *value* days
        before extracting features to optimize process.
        """
        super().__init__(hydro_coords, extractor)
        self.days_usage_config = days_usage_config

    def extract(self, water_levels):
        levels_stats = self._levels_stats(water_levels)
        lags = self._lags(water_levels)
        diff_features = self._diff_features(water_levels)
        doy_stats = self._doy_stats(water_levels)

        extracted_features = [levels_stats, lags, diff_features, doy_stats]
        extracted_features_last_date = self._keep_last_date(extracted_features)

        df_all_features = self._unite_features(extracted_features_last_date)
        return df_all_features

    def get_filtered_levels(self, water_levels, usage_config_key):
        days_needed = self.days_usage_config[usage_config_key]
        filtered_levels = self.filter_df_latest_n_days(water_levels, days_needed)
        return filtered_levels

    def filter_df_latest_n_days(self, df, n):
        if n is None:
            return df

        dates = df.reset_index()["date"]
        last_n_uniq_dates = sorted(dates.unique())[-n:]

        mask_last_n_dates = dates.isin(last_n_uniq_dates).values
        return df[mask_last_n_dates]

    def _levels_stats(self, water_levels):
        stats_levels = self.get_filtered_levels(water_levels, "levels_stat")
        return self.extractor.levels_stats(stats_levels)

    def _lags(self, water_levels):
        lags_levels = self.get_filtered_levels(water_levels, "lags")
        return self.extractor.lags(lags_levels)

    def _diff_features(self, water_levels):
        diff_levels = self.get_filtered_levels(water_levels, "diff")
        return self.extractor.diff_features(diff_levels)

    def _doy_stats(self, water_levels):
        doy_levels = self.get_filtered_levels(water_levels, "doy")
        return self.extractor.doy_stats(doy_levels)

    def _keep_last_date(self, dfs):
        new_dfs = []
        for df in dfs:
            # print(df)
            df_last = self.filter_df_latest_n_days(df, 1)
            new_dfs.append(df_last)

        return new_dfs