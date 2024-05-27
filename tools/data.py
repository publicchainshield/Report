import dask.dataframe as dd


def get_source():
    folder_path = ''

    ddf = dd.read_csv(f"{folder_path}/*.part")

    return ddf
