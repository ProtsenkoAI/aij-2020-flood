def groupby_station_date(df):
    df = df.reset_index()
    df["date"] = df["datetime"].dt.date
    df.drop(columns=["datetime"], inplace=True)

    grouped = df.groupby(by=["stationNumber", "date"])
    return grouped
