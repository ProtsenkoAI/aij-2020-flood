class MeteoManager:
    def __init__(self, past_loader, forecast_loader, past_preprocessor, forecast_preprocessor, extract_manager):
        self.past_loader = past_loader
        self.forecast_loader = forecast_loader

        self.past_preprocessor = past_preprocessor
        self.forecast_preprocessor = forecast_preprocessor

        self.extract_manager = extract_manager

        self.hydro = None
        self.id_colname = "stationNumber"

    def make_past_features(self):
        meteo = self.past_loader.load()
        self.needed_ids = self._get_needed_ids(meteo)
        meteo_preprocessed = self.past_preprocessor.preprocess(meteo)
        meteo_features = self.extract_manager.extract(meteo_preprocessed)
        return meteo_features

    def forecast_features(self, new_day):
        raw_forecast = self.forecast_loader.load(new_day, self.needed_ids)
        preprocessed = self.forecast_preprocessor.preprocess(raw_forecast) #!!!!!!!!!: смержить с meteo из train
        features = self.extract_manager.extract(preprocessed)

        return features

    def get_forecast_date(self, forecast, date):
        forecast_date = forecast.reset_index()["date"]
        mask_needed_date = forecast_date == date

        return forecast[mask_needed_date]

    def _get_needed_ids(self, meteo):
        id_col_vals = meteo.reset_index()[self.id_colname]
        return id_col_vals.unique()
