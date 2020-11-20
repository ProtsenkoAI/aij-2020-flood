from datetime import timedelta


class StationFeatureManager:
    def __init__(self, hydro_manager, meteo_manager, data_builder):
        self.hydro_manager = hydro_manager
        self.meteo_manager = meteo_manager
        self.data_builder = data_builder

        self.forecast_date = None

    def get_whole_past(self):
        hydro_features = self.hydro_manager.make_past_feautes()
        meteo_features = self.meteo_manager.make_past_features()

        whole_past = self.data_builder.build(hydro_features, meteo_features)
        return whole_past

    def forecast_meteo(self, start_date):
        self.forecast_date = start_date
        return self.meteo_manager.forecast_features(start_date)

    def get_next_day_forecast(self, forecasted_meteo, next_day_water_levels):
        hydro_last_date = self.hydro_manager.make_new_day_features(next_day_water_levels, self.forecast_date)
        meteo_last_date = self.meteo_manager.get_forecast_date(forecasted_meteo, self.forecast_date)

        self.forecast_date += timedelta(days=1)

        return hydro_last_date, meteo_last_date
