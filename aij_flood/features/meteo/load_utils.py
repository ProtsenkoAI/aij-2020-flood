import os
import pandas as pd

from features.utils import reduce_memory_usage


def get_dir_files(dir_path):
    dir_files = []

    for obj_path in os.listdir(dir_path):
        obj_full_path = os.path.join(dir_path, obj_path)
        if os.path.isfile(obj_full_path):
            dir_files.append(obj_full_path)

    return dir_files


def filter_extension(filepathes, extension: str):
    def _check_extension(filename):
        file_ext = filename.split(".")[-1]
        return file_ext == extension  # returned when wasn't found

    found_files = list(filter(_check_extension, filepathes))
    return found_files


def read_csvs(pathes, reduce_mem=True):
    dataframes = []
    for file_path in pathes:
        df = pd.read_csv(file_path, index_col=0)

        if reduce_mem:
            df = reduce_memory_usage(df)

        dataframes.append(df)

    return dataframes


def concat(dfs):
    return pd.concat(dfs, axis=0)