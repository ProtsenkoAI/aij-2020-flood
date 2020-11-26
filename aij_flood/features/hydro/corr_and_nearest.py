import numpy as np
import pandas as pd
# TODO: refactor
from tqdm import tqdm

class CorrAndNearest:
    def __init__(self, water_levels, hydro_coords):
        self.water_levels = water_levels
        self.hydro_coords = hydro_coords

        self.SHIFTS = np.arange(-5, 1)
        self.hydro_ids = self.water_levels.index.get_level_values("id").unique()
        self.hydro_number = len(self.hydro_ids)

    def get_corr_and_nearest(self):
        hydro_distances = self.calc_coordinate_distance_matrix(self.hydro_coords)
        # plt.imshow(hydro_distances)
        nearest_posts = self.find_nearest_posts(hydro_distances, hydro_distances)
        nearest_posts.head()

        corr_results = self.calc_correlation_matrix(self.water_levels, self.SHIFTS)
        top_corrs = self.find_most_corr_posts(self.hydro_ids, corr_results, self.SHIFTS, hydro_distances, max_dist=2.2)
        top_corrs.head()

        index_post_ids = top_corrs.index.to_series().apply(lambda x: self.hydro_ids[x])
        top_corrs.index = index_post_ids

        best_corr_ids = top_corrs["best_corr_idx"].apply(lambda x: self.hydro_ids[x])
        top_corrs["best_corr_idx"] = best_corr_ids
        top_corrs.rename(columns={"best_corr_idx": "best_corr_post", "shift": "best_corr_shift"}, inplace=True)

        corr_and_nearest = top_corrs.merge(nearest_posts, how="right", on="post")
        corr_and_nearest.head()

        corr_and_nearest["nearest_shift"] = np.full(len(corr_and_nearest), -1)

        corr_and_nearest["best_corr_dist"] = self.get_distances(hydro_distances, corr_and_nearest.index,
                                                           corr_and_nearest["best_corr_post"])
        corr_and_nearest["best_corr_value"] = self.get_correlations(corr_results, corr_and_nearest.index,
                                                               corr_and_nearest["best_corr_post"],
                                                               corr_and_nearest["best_corr_shift"])

        corr_and_nearest["nearest_dist"] = self.get_distances(hydro_distances, corr_and_nearest.index,
                                                         corr_and_nearest["nearest_post"])
        corr_and_nearest["nearest_corr"] = self.get_correlations(corr_results, corr_and_nearest.index,
                                                            corr_and_nearest["nearest_post"],
                                                            corr_and_nearest["nearest_shift"])

        return corr_and_nearest

    def calc_euclidean_distance(self, sample, pairs_arr):
        return (((sample - pairs_arr) ** 2).sum(axis=1))**0.5

    def calc_distances_with_all_hydro_points(self, sample):
        coords = sample[["latitude", "longitude"]]
        df_coords = self.hydro_coords[["latitude", "longitude"]]
        return self.calc_euclidean_distance(coords, df_coords)

    def calc_coordinate_distance_matrix(self, df):
        return df.apply(lambda x: self.calc_distances_with_all_hydro_points(x), axis=1)

    def calc_correlation_matrix(self, df, shifts):
        corr_results = np.full((len(shifts), self.hydro_number, self.hydro_number), np.nan, dtype=float)

        for shift_idx, shift in enumerate(shifts):
            for idx_first_post, hydro_post in tqdm(enumerate(self.hydro_ids), total=self.hydro_number):
                sample = df.loc[hydro_post, "max_level"]
                shifted_sample = self.shift_date_index(sample, shift)

                filtered_df = self.preprocess_df_for_corr(sample, df)
                correlations = self.calc_correlations(sample, filtered_df)

                for (second_post, corr_value) in correlations.items():
                    idx_second_post = self.hydro_ids.get_loc(second_post)
                    corr_results[shift_idx, idx_first_post, idx_second_post] = corr_value

        return corr_results

    def shift_date_index(self, df, days, copy=False):
        if copy:
            df = df.copy()
        df.index += pd.to_timedelta(days, unit='d')
        return df

    def preprocess_df_for_corr(self, sample, df, min_records=70):
        #     right_triangle_df = filter_min_id(df, hydro_post) # keep only right triangle of matrix to reduce computational time
        right_dates = sample.index
        df_same_dates = self.filter_dates(df, right_dates)
        df_enough_records = self.filter_min_records_count(df_same_dates, min_records)

        return df_enough_records

    def calc_correlations(self, first_hydro_post, all_hydro_posts):
        post_ids = self.get_ids(all_hydro_posts).unique()
        correlations = {}

        for second_post_id in post_ids:
            second_hydro_post = all_hydro_posts.loc[second_post_id, "max_level"]

            corr = first_hydro_post.corr(second_hydro_post)
            correlations[second_post_id] = corr

        return correlations

    def filter_dates(self, df, right_dates):
        df_dates = self.get_dates(df)
        date_correct_mask = df_dates.isin(right_dates)
        df = df[date_correct_mask]
        return df

    def filter_min_records_count(self, df, min_count):
        df_ids = self.get_ids(df)
        records_by_id = df_ids.value_counts()

        df_filtered_ids = records_by_id[records_by_id >= min_count].index
        df = df[df_ids.isin(df_filtered_ids)]
        return df

    def get_ids(self, df):
        return df.index.get_level_values("id")

    def get_dates(self, df):
        return df.index.get_level_values("date")

    def set_diag_val(self, array, value):
        diag_mask = np.diag(np.ones(array.shape[-1], dtype=bool))
        array[diag_mask] = value
        return array

    def find_nearest_posts(self, posts, distance_matrix):
        distance_matrix = self.set_diag_val(distance_matrix, np.nan)
        post_to_nearest = {}

        for post in posts:
            nearest = self.min_idx(distance_matrix[post])
            post_to_nearest[post] = {"nearest_post": nearest}

        nearest_posts = pd.DataFrame.from_dict(data=post_to_nearest, orient="index")
        nearest_posts.index.name = "post"
        return nearest_posts

    def min_idx(self, arr):
        min_val_idx = np.nanargmin(arr)
        arr_idx_min_val = arr.index[min_val_idx]
        return arr_idx_min_val

    def find_most_corr_posts(self, posts, corr_matrix, shifts, distances, max_dist=8.0):
        post_to_parent = {}
        corr_matrix = corr_matrix.copy()  # copy to prevent modyfying

        corr_matrix = self.set_diag_val_3d(corr_matrix, np.nan)
        mask_far_points = self.get_far_points_mask(distances, max_dist)
        corr_matrix[:, mask_far_points] = np.nan

        while self.get_posts_not_in_dict(posts, post_to_parent):
            if self.check_arr_only_nan(corr_matrix):
                break

            shift_idx, shifted_post, parent_post = self.max_coords_3d(corr_matrix)
            shift = shifts[shift_idx]

            if self.check_no_zero_loop(post_to_parent, shift, shifted_post, parent_post):
                post_to_parent[shifted_post] = {"id": parent_post, "shift": shift}
                corr_matrix[:, shifted_post, :] = np.nan

        #             corr_matrix[:, parent_post, shifted_post] = np.nan

        top_corrs = pd.DataFrame.from_dict(data=post_to_parent, orient="index")
        top_corrs.index.name = "post"
        top_corrs.rename(columns={"id": "best_corr_idx"}, inplace=True)

        return top_corrs

    def check_no_zero_loop(self, post_to_parent, shift, post, parent_post):
        total_shift = shift
        visited = [post]

        while post in post_to_parent:
            parent_post, shift = post_to_parent[post]["id"], post_to_parent[post]["shift"]
            total_shift += shift

            if parent_post in visited:
                if total_shift == 0:
                    return False
                else:
                    return True

            visited.append(parent_post)
            post = parent_post

        return True

    def max_coords_3d(self, array):
        max_val = np.nanmax(array)  # if using ordinal max, produces nan
        return np.argwhere(array == max_val)[0]

    def check_arr_only_nan(self, arr):
        if np.sum(~np.isnan(arr)):
            return False
        return True

    def set_diag_val_3d(self, array, value):
        diag_mask = np.diag(np.ones(array.shape[-1], dtype=bool))
        array[:, diag_mask] = value
        return array

    def values_to_idxs(self, seq, values):
        if not isinstance(seq, pd.Series):
            seq = pd.Series(seq)
        seq = pd.Index(seq)

        return [seq.get_loc(val) if not np.isnan(val) else np.nan for val in values]

    def get_arr_values_by_idxs(self, arr, idxs):
        vals = []
        for val_idx in idxs:
            if np.isnan(val_idx).any():
                val = np.nan

            else:
                val_idx = tuple([int(idx) for idx in val_idx])
                val = arr[val_idx]

            vals.append(val)

        return vals

    def get_distances(self, distances, first_posts, second_posts):
        first_posts = self.values_to_idxs(self.hydro_ids, first_posts)
        second_posts = self.values_to_idxs(self.hydro_ids, second_posts)

        dist_idxs = zip(first_posts, second_posts)
        return self.get_arr_values_by_idxs(distances.values, dist_idxs)

    def get_correlations(self, correlations, first_posts, second_posts, shifts):
        first_posts = self.values_to_idxs(self.hydro_ids, first_posts)
        second_posts = self.values_to_idxs(self.hydro_ids, second_posts)
        shifts = self.values_to_idxs(self.SHIFTS, shifts)

        corr_idxs = zip(shifts, first_posts, second_posts)
        return self.get_arr_values_by_idxs(correlations, corr_idxs)

    def get_far_points_mask(self, distances, max_val):
        mask_far_points = distances > max_val
        return mask_far_points

    def get_posts_not_in_dict(self, posts, dct):
        not_in_dct = [post for post in posts if post not in dct.keys()]
        return not_in_dct
