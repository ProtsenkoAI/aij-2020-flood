# TODO: check fill_forecast_nan() from meteo_extraction.ipynb

import xarray
import pandas as pd


class ForecastParser:
    def parse(self, forecast, var_names):
        print("parsing")
        self.data = xarray.backends.NetCDF4DataStore(forecast)

        values = {}
        for name in var_names:
            var_value = self._var_val(name)
            values[name] = var_value

        index = self._get_timestamps()
        df = pd.DataFrame.from_dict(values, orient="columns")
        df.index = index

        return df

    def _var_val(self, name):
        values = self.data.get_variables()[name].values
        return self._reshape_var_val(values)

    def _reshape_var_val(self, vals):
        shape = vals.shape

        if len(shape) == 3:
            vals = vals[:, :, 0]
        elif len(shape) > 3:
            raise ValueError("В Forecast размерность values > 3")

        return vals.flatten()[:-1]  # OMG WTF DOES THIS SLICE MEAN!? check later and clean code

    def _get_timestamps(self):
        attrs = self.data.get_attrs()
        start_time = pd.to_datetime(attrs["time_coverage_start"])

        hours_from_start = self.data.get_variables()["time"]
        time_deltas = self._timedelta_from_hours(hours_from_start)[:-1]  # same slice

        timestamps = start_time + time_deltas
        timestamps.name = "datetime"

        return timestamps

    def _timedelta_from_hours(self, hours):
        return pd.to_timedelta(hours.values.flatten(), "h")
