import pandas as pd

from . import funcs


class Extractor:
    def __init__(self, extract_config):
        try:
            self.lag_days = extract_config["lags"]
            self.diff_lag_days = extract_config["diff_lags"]
            self.diff_funcs = extract_config["diff_funcs"]
            self.levels_stat_config = extract_config["levels_stat_config"]
            self.past_years_funcs = extract_config["past_years_funcs"]

        except KeyError:
            raise ValueError("extract config doesn't contain required field(s)")

    def levels_stats(self, df):
        print(self.levels_stat_config)
        colnames = [f"water_levels_{feature[0].__name__}_{feature[1]}_{feature[2]}" for feature in self.levels_stat_config]
        print("colnames", colnames)
        levels_stats = funcs.stats(df, self.levels_stat_config)
        levels_stats.columns = colnames
        return levels_stats

    def lags(self, df):
        print(self.lag_days)
        colnames = [f"lag{feature}" for feature in self.lag_days]
        colnames = ["lag1", "lag2", "lag3", "lag4", "lag5", "lag6", "lag7"]
        lags = funcs.lags(df, self.lag_days)
        lags.columns = colnames
        return lags

    def diff_features(self, df):
        diff_lags = funcs.diff_lag(df, self.diff_lag_days)
        diff_stats = funcs.diff_stats(diff_lags, self.diff_funcs)
        diff_features = pd.concat([diff_lags, diff_stats], axis=1)
        return diff_features

    def doy_stats(self, df):
        return funcs.past_years_stats(df, self.past_years_funcs)



# df = self.water_levels
# levels_stats = extractor.levels_stats(df, self.levels_stat_config)
# lags = extractor.lags(df, self.lag_days)
# diff_features = extractor.diff_features(df, self.diff_lag_days, self.diff_funcs)
# past_years_stats = extractor.past_years_stats(df, self.past_years_funcs)
#
# extracted_features = [levels_stats, lags, diff_features, past_years_stats]
# all_features = self._unite_features(extracted_features)
# return all_features