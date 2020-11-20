from datetime import timedelta


def data_add_day(hydro_df, new_day_values):
    hydro_df = hydro_df.append(new_day_values)
    return hydro_df.sort_index()