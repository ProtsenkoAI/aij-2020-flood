from .extractor import Extractor
import pandas as pd


class ExtractManager:
    def __init__(self, extractor: Extractor, last_day_days_usage_config):
        """
        :param last_day_days_usage_config: mapping with keys ("lags", "diff", "levels_stats", "doy")
        and values containing int. Values'll be used to filter water_levels by last *value* days
        before extracting features to optimize process.
        """
        # self.hydro_coords = hydro_coords
        self.extractor = extractor
        self.last_day_days_usage_config = last_day_days_usage_config

    def extract(self, water_levels):
        print("start hydro extraction")
        water_levels = self.fill_missing_dates(water_levels)
        print("water_levels extract", water_levels)
        print("filled missing dates")
        extracted_features = self._extract_all_features_list(water_levels)
        all_features = self._unite_features(extracted_features)
        return all_features

    def extract_last_day(self, water_levels):
        extracted = self._extract_last_day_features_list(water_levels)
        last_day_features = self._unite_features(extracted)
        return last_day_features

    def _extract_all_features_list(self, water_levels):
        # water_levels.rename(columns={"max_level": "target"}, inplace=True)
        print(water_levels.head(1))
        print("extracting levels stats")
        levels_stats = self._levels_stats(water_levels)
        print("levels stats", levels_stats)
        print("lags turn")
        lags = self._lags(water_levels)
        print("lags", lags)
        diff_features = self._diff_features(water_levels)
        doy_stats = self._doy_stats(water_levels)

        return [levels_stats, lags, diff_features, doy_stats, water_levels] # add water_levels?

    def _extract_last_day_features_list(self, water_levels):
        stats_levels = self._get_filtered_levels(water_levels, "levels_stats")
        levels_stats = self._levels_stats(stats_levels)

        lags_levels = self._get_filtered_levels(water_levels, "lags")
        lags = self._lags(lags_levels)

        diff_levels = self._get_filtered_levels(water_levels, "diff_features")
        diff_features = self._diff_features(diff_levels)

        doy_levels = self._get_filtered_levels(water_levels, "doy")
        doy_stats = self._doy_stats(doy_levels)

        return self._keep_last_date([levels_stats, lags, diff_features, doy_stats])

    def _get_filtered_levels(self, water_levels, usage_config_key):
        days_needed = self.last_day_days_usage_config[usage_config_key]
        filtered_levels = self.filter_df_latest_n_days(water_levels, days_needed)
        return filtered_levels

    def filter_df_latest_n_days(self, df, n):
        if n is None:
            return df

        dates = df.reset_index()["date"]
        last_n_uniq_dates = sorted(dates.unique())[-n:]

        mask_last_n_dates = dates.isin(last_n_uniq_dates).values
        return df[mask_last_n_dates]

    def _levels_stats(self, df):
        return self.extractor.levels_stats(df)

    def _lags(self, df):
        return self.extractor.lags(df)

    def _diff_features(self, df):
        return self.extractor.diff_features(df)

    def _doy_stats(self, df):
        return self.extractor.doy_stats(df)

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

    def _keep_last_date(self, dfs):
        new_dfs = []
        for df in dfs:
            # print(df)
            df_last = self.filter_df_latest_n_days(df, 1)
            new_dfs.append(df_last)

        return new_dfs

    def fill_missing_dates(self, df):
        dates = df.reset_index()["date"]
        ids = df.reset_index()["id"].unique()
        new_date_index = pd.date_range(dates.min(), dates.max())

        all_dates_index = pd.MultiIndex.from_product([ids, new_date_index], names=["id", "date"])
        return df.reindex(all_dates_index)

