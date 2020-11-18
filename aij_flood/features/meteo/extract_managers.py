from . import extr_utils
import numpy as np
# TODO: сделать эксрактор по сути набором функций, здесь хранить какие колонки извлекать
# ,конфиги для них, логику генерации имён для фич


class DirMeteoExtractManager:
    def __init__(self, preprocessed_meteo, extractor):
        self.meteo = preprocessed_meteo
        self.extractor = extractor

    def extract(self):
        self.meteo_agg_daily()

    def meteo_agg_daily(self):
        grouped = extr_utils.groupby_station_date(self.meteo)
        daily_mean = grouped.agg(np.nanmean)
        daily_mean.set_index(["stationNumber", "date"])

        self.meteo = daily_mean
