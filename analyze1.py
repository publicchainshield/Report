import csv
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from dask.dataframe import dd

to_path = '3_sybil_report_data/result'
final_path = '3_sybil_report_data/final'
source_path = 'temp_done.csv'
merge_path = 'merged_output.csv'
transfer_path = 'transfer_time.csv'


def process_csv():
    # CSV
    file_path = '3_sybil_report_data/source/clear_transactions/*.part'
    ddf = dd.read_csv(file_path).dropna(subset=['PROJECT'])

    ddf = ddf.compute()
    ddf['SENDER_WALLET'] = ddf['SENDER_WALLET'].astype(str)
    ddf['SOURCE_TIMESTAMP_UTC'] = dd.to_datetime(ddf['SOURCE_TIMESTAMP_UTC'])

    print(1)

    tx_number = ddf.groupby('SENDER_WALLET').size().reset_index()
    tx_number.columns = ['SENDER_WALLET', 'tx_number']

    print(2)

    # tx_number3
    tx_number = tx_number[tx_number['tx_number'] > 3]

    print(3)

    # tx_numberddf
    ddf = ddf.merge(tx_number[['SENDER_WALLET']], on='SENDER_WALLET', how='inner')

    print(4)

    # project_number
    project_number = ddf.groupby('SENDER_WALLET').apply(lambda x: len(set(x['PROJECT']))).reset_index()
    project_number.columns = ['SENDER_WALLET', 'project_number']

    print(5)

    # earliest_datelatest_date
    earliest_date = ddf.groupby('SENDER_WALLET')['SOURCE_TIMESTAMP_UTC'].min().reset_index()
    earliest_date.columns = ['SENDER_WALLET', 'earliest_date']

    print(6)

    latest_date = ddf.groupby('SENDER_WALLET')['SOURCE_TIMESTAMP_UTC'].max().reset_index()
    latest_date.columns = ['SENDER_WALLET', 'latest_date']

    print(7)

    def process(x):
        # SOURCE_TIMESTAMP_UTC
        second_min_timestamp_row = x.nsmallest(10, 'SOURCE_TIMESTAMP_UTC')

        # PROJECT
        second_min_project = second_min_timestamp_row['PROJECT']
        second_min_timestamp = second_min_timestamp_row['SOURCE_TIMESTAMP_UTC']

        # Series
        re = []
        for i in range(len(second_min_project)):
            re.append(second_min_project.iloc[i])
            re.append(second_min_timestamp.iloc[i])

        return re

    history = ddf.groupby('SENDER_WALLET').apply(lambda x: process(x)).reset_index()
    history.columns = ['SENDER_WALLET', 'history']

    print(8)

    # 
    result = tx_number.merge(project_number, on='SENDER_WALLET')
    result = result.merge(earliest_date, on='SENDER_WALLET')
    result = result.merge(latest_date, on='SENDER_WALLET')
    result = result.merge(history, on='SENDER_WALLET')

    print(9)

    # CSV
    result.to_csv('3_sybil_report_data/final/grouped_results.csv', index=False)

    print("Result saved to '3_sybil_report_data/final/grouped_results.csv'")


def group_by_projects_final():
    import pandas as pd
    df = pd.read_csv("3_sybil_report_data/final/grouped_results.csv")
    # time
    df['earliest_date'] = pd.to_datetime(df['earliest_date'])
    df['latest_date'] = pd.to_datetime(df['latest_date'])

    # time "" 
    df['earliest_date'] = df['earliest_date'].dt.strftime('%Y-%m-%d')
    df['latest_date'] = df['latest_date'].dt.strftime('%Y-%m-%d')

    def result(s):
        if s.shape[0] < 20:
            return

        s.to_csv(f'3_sybil_report_data/final/filter_by_projects_details/group_{s.name}.csv', index=False)

    df.groupby(['tx_number', 'project_number', 'earliest_date', 'latest_date']).apply(
        lambda s: {result(s)})


# ok
def filter_by_time():
    # CSV
    input_file_path = '350w.csv'

    # CSV
    df = dd.read_csv(input_file_path)

    # NaN
    cleaned_df = df.dropna()

    # 
    def convert_to_date(ts):
        return pd.to_datetime(ts, unit='ms').date()

    # last_tx_timestampfirst_tx_timestamp
    cleaned_df['last_tx_date'] = cleaned_df['last_tx_timestamp'].map_partitions(lambda s: s.apply(convert_to_date),
                                                                                meta=('last_tx_date', 'datetime64[ns]'))
    cleaned_df['first_tx_date'] = cleaned_df['first_tx_timestamp'].map_partitions(lambda s: s.apply(convert_to_date),
                                                                                  meta=(
                                                                                      'first_tx_date',
                                                                                      'datetime64[ns]'))

    def result(s, path):
        if s.shape[0] >= 5:
            s.to_csv(f'{to_path}/{path}/group_{str(s.name)
                     .replace('(', '_')
                     .replace(')', '_')
                     .replace("'", '_')
                     .replace(' ', '_')
                     .replace(':', '_')}.csv', index=False)

    # first_tx_datelast_tx_date，
    grouped = cleaned_df.groupby(['first_tx_date', 'last_tx_date', 'CHAIN', 'transaction_count']).apply(
        lambda s: {result(s, 'filter_by_time')}).compute()


def filter_by_time_no_same_day():
    import os
    import re
    import pandas as pd

    # 
    folder_path = '3_sybil_report_data/result/filter_by_time'
    output_csv_path = 'mismatched_files.csv'

    # 
    file_data = []

    # 
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            file_data.append((filename, file_size))

    # DataFrame
    df = pd.DataFrame(file_data, columns=['Filename', 'Size'])
    df_sorted = df.sort_values(by='Size', ascending=False)

    # 
    date_pattern = re.compile(r'datetime\.date_(\d+),_(\d+),_(\d+)_,')

    # 
    mismatched_files = []

    # 
    for filename in df_sorted['Filename']:
        # 
        dates = date_pattern.findall(filename)
        if len(dates) == 2:
            date1 = dates[0]
            date2 = dates[1]
            # ，，
            if date1 != date2:
                mismatched_files.append(filename)

    # CSV
    if mismatched_files:
        mismatched_df = df_sorted[df_sorted['Filename'].isin(mismatched_files)]
        mismatched_df.to_csv(output_csv_path, index=False)
        print(f"Mismatched files saved to {output_csv_path}")
    else:
        print("No mismatched files found.")


# okbridge
def get_detail():
    import os
    import pandas as pd

    # 
    file_a_path = 'filename.csv'
    folder_b_path = '3_sybil_report_data/result/filter_by_time'
    folder_c_path = '3_sybil_report_data/result/factor_group_by_wallet'
    output_csv_path = 'output.csv'

    # A
    file_a_df = pd.read_csv(file_a_path, header=None)

    # 
    output_data = []

    # A
    for index, row in file_a_df.iterrows():
        d = row[0].strip('"')

        # Bd
        file_b_path = os.path.join(folder_b_path, d)
        if os.path.exists(file_b_path):
            file_b_df = pd.read_csv(file_b_path, header=None)
            e = file_b_df.iloc[1, 0]  # 
            f = len(file_b_df) - 1  # 

            # Cgroup_e.csv
            file_c_name = f'group_{e}.csv'
            file_c_path = os.path.join(folder_c_path, file_c_name)
            if os.path.exists(file_c_path):
                file_c_df = pd.read_csv(file_c_path, header=None)
                g = len(file_c_df)  # 

                # d, g, f
                output_data.append([d, g, f])

    # CSV
    output_df = pd.DataFrame(output_data, columns=['Filename', 'Group_Row_Count', 'File_Row_Count'])
    output_df.to_csv(output_csv_path, index=False)

    print(f"Output data saved to {output_csv_path}")


def get_bridge_details():
    def process_file(sender_wallet, folder_path):
        file_path = os.path.join(folder_path, f'group_{sender_wallet}.csv')

        if os.path.exists(file_path):
            group_df = pd.read_csv(file_path)
            group_df['SOURCE_TIMESTAMP_UTC'] = pd.to_datetime(group_df['SOURCE_TIMESTAMP_UTC'], errors='coerce')
            sorted_group_df = group_df.sort_values(by='SOURCE_TIMESTAMP_UTC').head(10)
            return sorted_group_df[['PROJECT', 'SOURCE_TIMESTAMP_UTC']]
        return pd.DataFrame(columns=['PROJECT', 'SOURCE_TIMESTAMP_UTC'])

    def add_project_and_timestamp_columns(base_file_path, folder_path, output_file_path):
        base_df = pd.read_csv(base_file_path)

        for i in range(1, 11):
            base_df[f'project_{i}'] = ''
            base_df[f'date_{i}'] = ''

        futures = []
        with ThreadPoolExecutor(max_workers=1000) as executor:
            for index, row in base_df.iterrows():
                sender_wallet = row['SENDER_WALLET']
                futures.append((index, executor.submit(process_file, sender_wallet, folder_path)))

            for index, future in futures:
                sorted_group_df = future.result()
                for idx, (project, timestamp) in enumerate(
                        zip(sorted_group_df['PROJECT'], sorted_group_df['SOURCE_TIMESTAMP_UTC'])):
                    base_df.at[index, f'project_{idx + 1}'] = project
                    base_df.at[index, f'date_{idx + 1}'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        base_df.to_csv(output_file_path, index=False)
        print(f"Result saved to {output_file_path}")

    # 
    base_file_path = 'group_by_tx_and_projects_and_day_days.csv'
    folder_path = '3_sybil_report_data/result/factor_group_by_wallet'
    output_file_path = 'group_by_projects_details.csv'

    # 
    add_project_and_timestamp_columns(base_file_path, folder_path, output_file_path)


process_csv()
