from copy import deepcopy
from typing import Dict, List, Set
import pandas as pd


def encode_transfers_data_by_coins(tokens_transfers: pd.DataFrame,
                                   wallet_addresses: Set,
                                   coins_symbols: Dict,
                                   coins_decimals: Dict
                                   ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    _zeroes = {address: [0 for coin in coins_symbols] for address in wallet_addresses}
    coins_sent = pd.DataFrame.from_dict(_zeroes, orient='index', columns=list(coins_symbols.values()))
    coins_received = pd.DataFrame.from_dict(_zeroes, orient='index', columns=list(coins_symbols.values()))
    n_coins_sent = pd.DataFrame.from_dict(_zeroes, orient='index', columns=list(coins_symbols.values()))
    n_coins_received = pd.DataFrame.from_dict(_zeroes, orient='index', columns=list(coins_symbols.values()))

    for i in tokens_transfers.index:
        coin_address = tokens_transfers['contract_address'][i]
        if coin_address in coins_symbols:
            coin_symbol = coins_symbols[coin_address]
            coin_decimals = coins_decimals[coin_address]

            if tokens_transfers['from'][i] in wallet_addresses:
                wallet_address = tokens_transfers['from'][i]
                coins_sent.loc[wallet_address, coin_symbol] += float(tokens_transfers['value'][i]) / 10**coin_decimals
                n_coins_sent.loc[wallet_address, coin_symbol] += 1

            elif tokens_transfers['to'][i] in wallet_addresses:
                wallet_address = tokens_transfers['to'][i]
                coins_received.loc[wallet_address, coin_symbol] += float(tokens_transfers['value'][i]) / 10**coin_decimals
                n_coins_received.loc[wallet_address, coin_symbol] += 1

        if not (i % 10000):
            print(f"Progress: {i} / {len(tokens_transfers)}")

    return coins_sent, n_coins_sent, coins_received, n_coins_received


def encode_transfers_data_by_coins_types(tokens_transfers: pd.DataFrame,
                                         wallet_addresses: Set,
                                         stable_coin_addresses: Set,
                                         meme_coin_addresses: Set) -> (pd.DataFrame, pd.DataFrame):
    transfers_sent_df = pd.DataFrame.from_dict(data={address: [0, 0, 0] for address in wallet_addresses},
                                               orient='index',
                                               columns=['coins_sent', 'stablecoins_sent', 'memecoins_sent'])

    transfers_received_df = pd.DataFrame.from_dict(data={address: [0, 0, 0] for address in wallet_addresses},
                                                   orient='index',
                                                   columns=['coins_received', 'stablecoins_received', 'memecoins_received'])

    for i in tokens_transfers.index:
        if tokens_transfers['from'][i] in wallet_addresses:
            wallet_address = tokens_transfers['from'][i]
            coin_address = tokens_transfers['contract_address'][i]
            transfers_sent_df.loc[wallet_address, 'coins_sent'] += 1
            if coin_address in stable_coin_addresses:
                transfers_sent_df.loc[wallet_address, 'stablecoins_sent'] += 1
            elif coin_address in meme_coin_addresses:
                transfers_sent_df.loc[wallet_address, 'memecoins_sent'] += 1

        elif tokens_transfers['to'][i] in wallet_addresses:
            wallet_address = tokens_transfers['to'][i]
            coin_address = tokens_transfers['contract_address'][i]
            transfers_received_df.loc[wallet_address, 'coins_received'] += 1
            if coin_address in stable_coin_addresses:
                transfers_received_df.loc[wallet_address, 'stablecoins_received'] += 1
            elif coin_address in meme_coin_addresses:
                transfers_received_df.loc[wallet_address, 'memecoins_received'] += 1

        if not (i % 10000):
            print(f"Progress: {i} / {len(tokens_transfers)}")

    return transfers_sent_df, transfers_received_df
