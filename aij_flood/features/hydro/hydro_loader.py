import pandas as pd
from .. import utils


class FileHydroLoader:
    def __init__(self, file_path):
        self.file_path = file_path

    def load(self):
        df = pd.read_csv(self.file_path, sep=";")
        df = utils.reduce_memory_usage(df)

        df.rename(columns={"identifier": "id", "time": "date", "max_level": "target"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"])

        df.set_index(["id", "date"], inplace=True)
        # df = self.fill_missing_dates(df)
        df = df.sort_index()

        return df

