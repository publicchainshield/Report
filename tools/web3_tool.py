import requests
import time
from datetime import datetime
from dotenv import load_dotenv
from eth_utils import to_checksum_address

from config.config import oklink_keys

load_dotenv()
network_replaced = {
    'Arbitrum': 'ARBITRUM',
    'Avalanche': 'AVAXC',
    'Base': 'BASE',
    'BNB Chain': 'BSC',
    'Canto': 'CANTO',
    'Ethereum': 'ETH',
    'Fantom': 'FTM',
    'Gnosis': 'GNOSIS',
    'Kava': 'KAVA',
    'Klaytn Mainnet Cypress': 'KLAYTN',
    'Linea': 'LINEA',
    'Manta': 'MANTA',
    'opBNB Mainnet': 'OPBNB',
    'Optimism': 'OP',
    'Polygon': 'POLYGON',
    'Polygon zkEVM': 'POLYGON_ZKEVM',
    'Scroll': 'SCROLL',
    'zkSync Era Mainnet': 'ZKSYNC',
}
network_valid = {
    'ARBITRUM',
    'AVAXC',
    'BASE',
    'BSC',
    'CANTO',
    'ETH',
    'FTM',
    'GNOSIS',
    'KAVA',
    'KLAYTN',
    'LINEA',
    'MANTA',
    'OPBNB',
    'OP',
    'POLYGON',
    'POLYGON_ZKEVM',
    'SCROLL',
    'ZKSYNC',
}

api_keys = oklink_keys


def make_request(endpoint, params, api_key, retries=2):
    if not network_valid.__contains__(params['chainShortName']):
        return None
    base_url = "https://www.oklink.com"
    proxies = {
    }
    headers = {
        "Ok-Access-Key": api_key
    }

    url = f"{base_url}/api/v5/explorer/address/{endpoint}"

    for _ in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, proxies=proxies)
            response.raise_for_status()
            return response.json().get("data", [None])[0]
        except requests.exceptions.HTTPError as e:
            # print(f"Failed to fetch data: {e}")
            time.sleep(1)
        except Exception as e:
            # print(f"Error: {e}")
            break
    return None


def information_evm(chain, address, api_key):
    params = {
        "chainShortName": network_replaced.get(chain, chain),
        "address": address,
    }
    return make_request("information-evm", params, api_key)


def recent_transaction_list(chain, address, api_key, page=1, limit=20):
    params = {
        "chainShortName": network_replaced.get(chain, chain),
        "address": address,
        "page": page,
        "limit": limit,
    }
    return make_request("transaction-list", params, api_key)


def first_transaction(chain, address, api_key):
    first_page_data = recent_transaction_list(chain, address, api_key, page=1, limit=1)
    if not first_page_data:
        return None

    if not first_page_data['totalPage']:
        return None

    total_pages = int(first_page_data['totalPage'])

    last_page_data = recent_transaction_list(chain, address, api_key, page=total_pages, limit=1)
    if not last_page_data:
        return None

    if 'transactionLists' in last_page_data and len(last_page_data['transactionLists']) > 0:
        return last_page_data['transactionLists'][0]

    return None


def get_account_info_and_age(chain, address, api_key):
    info = information_evm(chain, address, api_key)
    first_transaction_data = first_transaction(chain, address, api_key)

    if info and first_transaction_data:
        timestamp1 = first_transaction_data["transactionTime"]
        timestamp2 = info["lastTransactionTime"]
        if timestamp2 == '' or timestamp2 == '':
            age = None
        else:
            age = round((int(timestamp2) - int(timestamp1)) / (1000 * 3600 * 24), 1)
        return {
            "last_tx_timestamp": info["lastTransactionTime"],
            "balance": info["balance"],
            "balance_symbol": info["balanceSymbol"],
            "transaction_count": info["transactionCount"],
            "first_tx_timestamp": timestamp1,
            "funding_details_from": first_transaction_data['from'],
            "funding_details_amount": first_transaction_data['amount'],
            "funding_details_symbol": first_transaction_data['transactionSymbol'],
            "funding_details_timestamp": timestamp1,
            "age": age
        }
    else:
        return None


for index, oklink_key in enumerate(oklink_keys):
    age = get_account_info_and_age('Arbitrum', '',
                                   oklink_key)
    print(index, age)
