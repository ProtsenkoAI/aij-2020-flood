import os
import pickle
import numpy as np
import re

from ctypes import cdll, CDLL # just weird garbage collection

def save_pickle(obj, path):
    dir_of_path = os.path.dirname(path)
    os.makedirs(dir_of_path, exist_ok=True)
    with open(path, "wb") as file:
        pickle.dump(obj, file)


def load_pickle(path):
    with open(path, "rb") as file:
        obj = pickle.load(file)
    return obj


def reduce_memory_usage(df, use_float16=False):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.        
    """
    start_mem = df.memory_usage().sum() / 1024**2
#     print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
    for col in df.columns:
        col_type = str(df[col].dtype)
        
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if re.findall("int", col_type):
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            elif re.findall("float", col_type):
                # have some issues with pd.DataFrame.pivot when using np.float16, so disabling by default
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max and use_float16:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
            else:
                continue
        else:
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
#     print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
#     print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    
    return df

def dataframe_garbage_collection():
    cdll.LoadLibrary("libc.so.6")
    libc = CDLL("libc.so.6")
    libc.malloc_trim(0)


def filter_by_substring(strings, substring):
    
    def _check_substring(string):
        return string.find(substring) != -1 # returned when wasn't found
    
    filtered_strings = list(filter(_check_substring, strings))
    return filtered_strings