import dask.dataframe as dd

from tools.check import get_sybil_set


def process_by_dask():
    filename = './source/2024-05-15-snapshot1_transactions.csv'
    ddf = dd.read_csv(filename)

    sybil_set = get_sybil_set()

    filtered_df = ddf[~ddf['SENDER_WALLET'].isin(sybil_set)]

    output_file = 'source/clear_transactions'

    filtered_df.to_csv(output_file, index=False)

    print(f"{output_file}")
