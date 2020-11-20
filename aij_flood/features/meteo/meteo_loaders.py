# TODO: refactor

from . import load_utils

import pandas as pd
import numpy as np
from datetime import datetime
from siphon.catalog import TDSCatalog
from time import time


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


class ForecastMeteoLoader:
    def __init__(self, start_date, end_date, meteo_coords, retrieved_vars, varnames_table: pd.DataFrame, parser):
        """
        :param varnames_table: df with 1 col containing source name and 2 col with forecast names
        :param meteo_coords: contains id, lon and lat cols
        """

        self.start_date = start_date
        self.end_date = end_date
        self.meteo_coords = meteo_coords
        self.parser = parser

        self.varnames_table = varnames_table

        self.forecast_url = "http://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p5deg/catalog.xml"
        self.catalog = TDSCatalog(self.forecast_url)

        self.ncss_client = self._get_remote_client()
        self.retrieved_vars = self._convert_var_names(retrieved_vars)

    def load(self):
        raw_forecasts = self._download_forecast()

        points_df = []
        for point_forecast in raw_forecasts:
            df_point = self.parser.parse(point_forecast, self.retrieved_vars)
            points_df.append(df_point)

        df = self._build_general_df(points_df)
        df = self._filter_dates(df)
        df = self._set_normal_colnames(df)
        return df

    def _get_remote_client(self):
        datasets = self.catalog.catalog_refs
        data_dates = self._extract_datasets_dates(datasets)
        idx_nearest_ds = self._nearest_date_idx(data_dates, self.start_date)
        nearest_dataset = datasets[idx_nearest_ds]

        ncss_client = nearest_dataset.follow().datasets[0].subset()  # clean later
        return ncss_client

    def _extract_datasets_dates(self, datasets):
        dates = []

        for ds_name in datasets.keys():
            str_date = ds_name.split("_")[3]
            ds_date = datetime.strptime(str_date, "%Y%m%d")

            dates.append(ds_date)

        return dates

    def _nearest_date_idx(self, dates, compared_date):
        time_diffs = []

        for data_date in dates:
            diff = abs(data_date - compared_date)
            time_diffs.append(diff)

        return np.argmin(time_diffs)

    def _download_forecast(self):
        point_outputs = []

        for _, point in self.meteo_coords.iterrows():
            query = self._build_base_query(point)
            print("get data", point)
            t1 = time()
            out = self.ncss_client.get_data(query)
            print("got data", time() - t1)
            point_outputs.append(out)

        return point_outputs

    def _build_base_query(self, point):
        query = self.ncss_client.query()
        query.variables(*self.retrieved_vars)
        query.time_range(self.start_date, self.end_date)
        query.lonlat_point(point["lon"], point["lat"])

        query.accept("netCDF4")

        return query

    def _convert_var_names(self, src_names):
        src2forecast_names = self.varnames_table.reset_index().set_index("src")
        new_names = [src2forecast_names.loc[name, "forecast"] for name in src_names]
        return new_names

    def _set_normal_colnames(self, df):
        forecast2src_names = self.varnames_table.reset_index().set_index("forecast")
        forecast_vars = forecast2src_names.index
        rename_dict = {forecast_name: forecast2src_names.loc[forecast_name, "src"] for forecast_name in forecast_vars}
        df.rename(columns=rename_dict, inplace=True)

        return df

    def _filter_dates(self, df):
        datetime = pd.DatetimeIndex(df.reset_index()["datetime"]).tz_localize(None)
        in_needed_range_mask = (datetime >= self.start_date) & (datetime <= self.end_date)

        return df[in_needed_range_mask]

    def _build_general_df(self, dfs):
        ids = list(self.meteo_coords["id"])

        for station_id, df in zip(ids, dfs):
            df["id"] = station_id

        general_df = pd.concat(dfs, axis=0)
        return general_df
