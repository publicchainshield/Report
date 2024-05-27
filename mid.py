from tools.multi_thread import start_multithreaded_processing

from tools.web3_tool import get_account_info_and_age

input_filename = 'address_ok_attributes.csv'
output_filename = 'address_ok_attributes_done.csv'


# # ，
# with open(output_filename, 'w') as f:
#     # 
#     f.write(
#         'SENDER_WALLET,CHAIN,last_tx_timestamp,balance,balance_symbol,transaction_count,first_tx_timestamp,funding_details_from,funding_details_amount,funding_details_symbol,funding_details_timestamp,age\n')


def get_ok_attributes(start):
    import pandas as pd

    # CSV
    chunksize = 1000  # 

    # CSV
    for i, chunk in enumerate(pd.read_csv(input_filename, chunksize=chunksize)):
        if i < start:
            continue
        else:
            print(i)
            processing = start_multithreaded_processing(chunk, get_account_info_and_age)
            processing.to_csv(output_filename, mode='a', header=False, index=False)


get_ok_attributes(3532)
