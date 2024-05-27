import threading

import pandas as pd

from config.config import oklink_keys


def worker(index, api_key, df_slice, func, result_dict):
    result_dict[index] = df_slice.apply(lambda row: func(row['CHAIN'], row['SENDER_WALLET'], api_key),
                                                 axis=1)


def start_multithreaded_processing(df, func):
    api_keys = oklink_keys

    chunk_size = len(df) // len(api_keys)
    threads = []
    result_dict = {}

    for index, api_key in enumerate(api_keys):
        start_idx = index * chunk_size
        end_idx = (index + 1) * chunk_size if index < len(api_keys) - 1 else len(df)
        df_slice = df.iloc[start_idx:end_idx].copy()
        thread = threading.Thread(target=worker, args=(index, api_key, df_slice, func, result_dict))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    reset_index = pd.concat([result_dict[index] for index in range(len(api_keys))]).reset_index(drop=True)

    df_dicts = pd.json_normalize(reset_index)

    df_reset_index = df.reset_index(drop=True)
    dicts_reset_index = df_dicts.reset_index(drop=True)
    df_concat = pd.concat([df_reset_index, dicts_reset_index], axis=1)
    return df_concat
