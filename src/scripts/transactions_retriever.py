from typing import Dict, List
from multithread_processing.base_job import BaseJob
import pandas as pd

from databases.mongodb import MongoDB

DECIMALS = 10**18


class TransactionsFeatures(BaseJob):
    def __init__(self, wallets_list: list, chain_id: str,
                 max_workers: int = 8, batch_size: int = 1000):
        self.wallets_list = wallets_list

        self.wallets_data = {
            wallet_addr: {
                'unique_sent': set(),
                'unique_received': set(),
                'eth_sent': {'count': 0, 'min': float('inf'), 'max': 0, 'sum': 0},
                'eth_received': {'count': 0, 'min': float('inf'), 'max': 0, 'sum': 0}
            }
            for wallet_addr in wallets_list
        }

        self.chain_id = chain_id
        self.mongodb = MongoDB(chain_id=chain_id)

        super().__init__(work_iterable=wallets_list,
                         max_workers=max_workers,
                         batch_size=batch_size)

    def _execute_batch(self, works):
        native_transactions = self.mongodb.get_native_transfers_relate_to_addresses(works)
        for tx in native_transactions:
            tx_value = float(tx['value']) / DECIMALS
            if tx['from_address'] in works:
                self._extract_data_from_tx(sent_or_received='sent',
                                           wallet_address=tx['from_address'],
                                           other_address=tx['to_address'],
                                           tx_value=tx_value)
            elif tx['to_address'] in works:
                self._extract_data_from_tx(sent_or_received='received',
                                           wallet_address=tx['to_address'],
                                           other_address=tx['from_address'],
                                           tx_value=tx_value)

    def _extract_data_from_tx(self, sent_or_received: str, wallet_address: str, other_address: str, tx_value):
        self.wallets_data[wallet_address][f'unique_{sent_or_received}'].add(other_address)
        self.wallets_data[wallet_address][f'eth_{sent_or_received}']['count'] += 1
        self.wallets_data[wallet_address][f'eth_{sent_or_received}']['sum'] += tx_value
        self.wallets_data[wallet_address][f'eth_{sent_or_received}']['min'] = min(self.wallets_data[wallet_address][f'eth_{sent_or_received}']['min'],
                                                                   tx_value)
        self.wallets_data[wallet_address][f'eth_{sent_or_received}']['max'] = max(self.wallets_data[wallet_address][f'eth_{sent_or_received}']['max'],
                                                                   tx_value)

    def return_result(self) -> pd.DataFrame:
        result = [
            {
                'address': address,
                # sent
                'unique_sent': len(wallet_datum['unique_sent']),
                'min_coin_sent': wallet_datum['eth_sent']['min'],
                'max_coin_sent': wallet_datum['eth_sent']['max'],
                'avg_coin_sent': 0,
                # received
                'unique_received': len(wallet_datum['unique_received']),
                'min_coin_received': wallet_datum['eth_received']['min'],
                'max_coin_received': wallet_datum['eth_received']['max'],
                'avg_coin_received': 0
            }
            for address, wallet_datum in self.wallets_data.items()
        ]

        for wallet_datum in result:
            if wallet_datum['min_coin_sent'] == float('inf'):
                wallet_datum['min_coin_sent'] = 0

            if wallet_datum['min_coin_received'] == float('inf'):
                wallet_datum['min_coin_received'] = 0

            address = wallet_datum['address']
            if self.wallets_data[address]['eth_received']['count'] > 0:
                wallet_datum['avg_coin_received'] = self.wallets_data[address]['eth_received']['sum'] / \
                                                self.wallets_data[address]['eth_received']['count']
            if self.wallets_data[address]['eth_sent']['count'] > 0:
                wallet_datum['avg_coin_sent'] = self.wallets_data[address]['eth_sent']['sum'] / \
                                                self.wallets_data[address]['eth_sent']['count']
        return pd.DataFrame.from_records(result)
