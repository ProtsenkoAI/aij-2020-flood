from . import utils
import numpy as np


class HydroManager:
    def __init__(self, loader, extract_manager):
        self.loader = loader
        self.extract_manager = extract_manager

        self.hydro = None

    def make_past_features(self):
        self.hydro = self.loader.load()
        features = self.extract_manager.extract(self.hydro)
        return features

    def make_new_day_features(self, new_day_features, new_day):
        # TODO: it's bad code, clean it
        if self.hydro is None:
            raise RuntimeError("make_new_day_features shouldn't be called earlier than make_past_features")
        assert self.given_next_day(new_day_features, new_day), "new_day_features are from wrong day"

        self.hydro = utils.data_add_day(self.hydro, new_day_features)
        last_day_features = self.extract_manager.extract_last_day(self.hydro)
        return last_day_features

    def given_next_day(self, df, date):
        df_date = df.reset_index()["date"]
        if np.all(df_date == date):
            return True
        return False