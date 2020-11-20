import pandas as pd
from .. import utils


class FileHydroLoader:
    def __init__(self, file_path):
        self.file_path = file_path

    def load(self):
        df = pd.read_csv(self.file_path, sep=";")
        df_low_memory = utils.reduce_memory_usage(df)
        return df_low_memory
