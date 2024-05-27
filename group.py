import pandas as pd
import shutil
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from dask.dataframe import dd

from tools.data import get_source

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# pd 1000，
def process():
    filename = 'source/2024-05-15-snapshot1_transactions.csv'
    df = pd.read_csv(filename, nrows=10000000)
    grouped = df.groupby('STARGATE_SWAP_USD')
    filtered_groups = {key: group for key, group in grouped if len(group) > 20}
    for key, group_df in filtered_groups.items():
        print(f"Group: {key}")
        if key > 1:
            filename = f"../result/factor1/group_{key}_data.csv"
            print(f"Saving Group {key} to {filename}")
            group_df.to_csv(filename, index=False)
            print("\n")


# 
def get_length():
    filename = 'source/2024-05-15-snapshot1_transactions.csv'
    df_chunk = pd.read_csv(filename, chunksize=10000000)
    re = 0
    index = 1
    for chunk in df_chunk:
        len_chunk = len(chunk)
        print(f"Length: {index} : {len_chunk}")
        index += 1
        re += len_chunk
    print(f"Length: {re}")


def get_filenames_in_directory():
    # 
    filenames = set()

    # 
    for root, dirs, files in os.walk('result/factor_group_by_wallet'):
        for file in files:
            file = file[len('group_'):-len('.csv')]
            filenames.add(file)
    return filenames


# worked_wallet = get_filenames_in_directory()


def result(s, path):
    if s.shape[0] <= 20:
        print(s.name)
        print(s.shape)
        s.to_csv(f'result/{path}/group_{s.name}.csv', index=False)


# dask，
def process_same():
    ddf = get_source()
    compute = ddf.groupby("STARGATE_SWAP_USD").apply(lambda s: {
        result(s)
    }).compute()


# ，
def process_wallet():
    ddf = get_source()
    compute = ddf.groupby("SENDER_WALLET").apply(lambda s: {
        result(s, 'factor_group_by_wallet')
    }).compute()


# 
def get_wallets():
    ddf = get_source()
    unique_senders = ddf['SENDER_WALLET'].drop_duplicates().compute()
    output_path = '3_sybil_report_data/result/wallets.csv'
    unique_senders.to_csv(output_path, index=False, header=False)
    print(" SENDER_WALLET :", output_path)


# 
def get_chains():
    ddf = get_source()

    # 2.  'SOURCE_CHAIN'  'DESTINATION_CHAIN' 
    source_chain = ddf['SOURCE_CHAIN'].drop_duplicates().compute()
    destination_chain = ddf['DESTINATION_CHAIN'].drop_duplicates().compute()

    # 3. 
    combined_unique_chains = pd.concat([source_chain, destination_chain]).drop_duplicates().dropna()

    # 4.  CSV 
    output_path = '3_sybil_report_data/result/chains.csv'
    combined_unique_chains.to_csv(output_path, index=False, header=False)

    print(":", output_path)


# 
def get_address_and_chains():
    ddf = get_source()

    # SENDER_WALLETSOURCE_CHAIN，
    grouped_sender_chain = ddf.groupby(['SENDER_WALLET', 'SOURCE_CHAIN']).size().compute()

    # SENDER_WALLETDESTINATION_CHAIN，
    grouped_sender_destination = ddf.groupby(['SENDER_WALLET', 'DESTINATION_CHAIN']).size().compute()

    # 
    merged_groups = set(grouped_sender_chain.index).union(set(grouped_sender_destination.index))

    print(len(merged_groups))

    # 
    with open('address_ok_attributes.csv', 'w') as file:
        for group in merged_groups:
            file.write(','.join(map(str, group)) + '\n')


# 
def get_address_and_bridge():
    ddf = get_source()

    # SENDER_WALLET、SOURCE_CHAINDESTINATION_CHAIN
    grouped = ddf.groupby(['SENDER_WALLET', 'SOURCE_CHAIN', 'DESTINATION_CHAIN'])

    # 
    group_size = grouped.size().reset_index().rename(columns={0: 'BRIDGE_TIMES'})

    # STARGATE_SWAP_USD
    group_sum = grouped['STARGATE_SWAP_USD'].sum().reset_index().rename(
        columns={'STARGATE_SWAP_USD': 'BRIDGE_VALUES'})

    # 
    result = dd.merge(group_size, group_sum, on=['SENDER_WALLET', 'SOURCE_CHAIN', 'DESTINATION_CHAIN'])

    compute = result.compute()
    # CSV
    compute.to_csv('address_bridge_attributes.csv', index=False)



