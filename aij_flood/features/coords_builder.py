from . import utils


class CoordsBuilder:
    def __init__(self, posts_df_path):
        self.posts = utils.load_pickle(posts_df_path)

        self.hydro_id_colname = "gidro"
        self.meteo_id_colname = "station_id"

    def coords_by_col_val(self, id_colname, needed_values):
        id_col_vals = self.posts[id_colname]
        needed_rows_mask = id_col_vals.isin(needed_values)
        needed_rows = self.posts[needed_rows_mask.values].reset_index(drop=True)

        coords_with_id_col = needed_rows[[id_colname, "lon", "lat"]]
        return coords_with_id_col

    def hydro_coords(self, needed_hydro_ids):
        return self.coords_by_col_val(self.hydro_id_colname, needed_hydro_ids)

    def meteo_coords(self, needed_meteo_ids):
        return self.coords_by_col_val(self.meteo_id_colname, needed_meteo_ids)