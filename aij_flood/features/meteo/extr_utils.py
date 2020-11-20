def groupby_station_date(df):
    df = df.reset_index()
    df["date"] = df["datetime"].dt.date
    df.drop(columns=["datetime"], inplace=True)

    grouped = df.groupby(by=["stationNumber", "date"])
    return grouped


def create_colname(col, *postfixes):
    col = str(col)
    for postfix in postfixes:
        col += f"_{postfix}"
    return col


def plural_create_colname(cols, *prefixes):
    new_cols = [create_colname(col, *prefixes) for col in cols]
    return new_cols
