from . import load_utils


class DirMeteoLoader:
    def __init__(self, dir_path):
        self.dir_path = dir_path

    def load(self):
        filepathes = self._find_data_paths()
        meteo_data = self._load_concat(filepathes)
        return meteo_data

    def _find_data_paths(self):
        all_files = load_utils.get_dir_files(self.dir_path)
        csv_files = load_utils.filter_extension(all_files, "csv")
        return csv_files

    def _load_concat(self, filepathes):
        dfs = load_utils.read_csvs(filepathes)
        concated_df = load_utils.concat(dfs)
        return concated_df
