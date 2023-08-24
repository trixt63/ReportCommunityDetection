from typing import Dict, List
from multithread_processing.base_job import BaseJob
import pandas as pd

from constants.network_constants import Chains
from databases.blockchain_etl import BlockchainETL
from databases.mongodb import MongoDB

DECIMALS = 10**18


class TransactionsRetriever(BaseJob):
    def __init__(self, wallets_list: list, end_block: int, chain_id: str,
                 max_workers: int = 16, batch_size: int = 1000):
        self.wallets_list = wallets_list
        self.end_block = end_block

        self.wallets_data = {
            wallet_addr: {
                'unique_sent': set(),
                'unique_received': set(),
                'eth_sent': {'count': 0, 'min': float('inf'), 'max': 0, 'sum': 0},
                'eth_received': {'count': 0, 'min': float('inf'), 'max': 0, 'sum': 0}
            }
            for wallet_addr in wallets_list
        }

        self.mongodb = MongoDB()

        self.chain_id = chain_id
        _db_prefix = ""
        if chain_id != '0x38':
            _db_prefix = Chains.names[chain_id]
        self.blockchain_etl = BlockchainETL(db_prefix=_db_prefix)

        super().__init__(work_iterable=list(range(self.end_block)),
                         max_workers=max_workers,
                         batch_size=batch_size)

    def _execute_batch(self, works):
        from_block = works[0]
        to_block = works[-1]
        for address in self.wallets_list:
            retrieved_transactions = list(self.blockchain_etl.get_transactions_relate_to_address(address=address,
                                                                                                 from_block=from_block,
                                                                                                 to_block=to_block))

            self.mongodb.update_transactions(chain_id=self.chain_id, data=retrieved_transactions)

            # sent_transactions = list(self.blockchain_etl.get_to_transactions(from_address=address, from_block=from_block, to_block=to_block))
            # received_transactions = list(self.blockchain_etl.get_from_transactions(to_address=address, from_block=from_block, to_block=to_block))

            # # Unique sent
            # unique_sent_addresses = {tx['to_address'] for tx in sent_transactions}
            # self.wallets_data[address]['unique_sent'].union(unique_sent_addresses)
            # # Native token amount sent
            # count_eth_sent = sum(1 for tx in sent_transactions if tx['value'] > 0)
            # if count_eth_sent > 0:
            #     tx_values = [float(tx['value']) / DECIMALS for tx in sent_transactions if tx['value'] > 0]
            #     sum_eth_sent = sum(tx_values)
            #     max_eth_sent = max(tx_values)
            #     min_eth_sent = min(tx_values)
            # else:
            #     sum_eth_sent = 0
            #     max_eth_sent = 0
            #     min_eth_sent = float('inf')
            # self.wallets_data[address]['eth_sent']['count'] += count_eth_sent
            # self.wallets_data[address]['eth_sent']['sum'] += sum_eth_sent
            # self.wallets_data[address]['eth_sent']['min'] = min(self.wallets_data[address]['eth_sent']['min'],
            #                                                     min_eth_sent)
            # self.wallets_data[address]['eth_sent']['max'] = max(self.wallets_data[address]['eth_sent']['max'],
            #                                                     max_eth_sent)
            #
            # # Unique received
            # unique_received_addresses = {tx['from_address'] for tx in received_transactions}
            # self.wallets_data[address]['unique_received'].union(unique_received_addresses)
            # # Native token amount received
            # count_eth_received = sum(1 for tx in received_transactions if tx['value'] > 0)
            # if count_eth_received > 0:
            #     tx_values = [float(tx['value'])/DECIMALS for tx in received_transactions if tx['value'] > 0]
            #     sum_eth_received = sum(tx_values)
            #     max_eth_received = max(tx_values)
            #     min_eth_received = min(tx_values)
            # else:
            #     sum_eth_received = 0
            #     max_eth_received = 0
            #     min_eth_received = float('inf')
            # self.wallets_data[address]['eth_received']['count'] += count_eth_received
            # self.wallets_data[address]['eth_received']['sum'] += sum_eth_received
            # self.wallets_data[address]['eth_received']['min'] = min(self.wallets_data[address]['eth_received']['min'],
            #                                                         min_eth_received)
            # self.wallets_data[address]['eth_received']['max'] = max(self.wallets_data[address]['eth_received']['max'],
            #                                                         max_eth_received)

        print(f"Calculate to block {from_block} to {to_block} / {self.end_block}")

    def return_result(self):
        result = [
            {
                'address': address,
                # sent
                'unique_sent': len(wallet_datum['unique_sent']),
                'min_coin_sent': wallet_datum['eth_sent']['min'],
                'max_coin_sent': wallet_datum['eth_sent']['max'],
                # 'avg_coin_sent': wallet_datum['eth_sent']['sum'] / wallet_datum['eth_sent']['count'],
                # received
                'unique_received': len(wallet_datum['unique_received']),
                'min_coin_received': wallet_datum['eth_received']['min'],
                'max_coin_received': wallet_datum['eth_received']['max'],
                # 'avg_coin_received': wallet_datum['eth_received']['sum'] / wallet_datum['eth_received']['count'],
            }
            for address, wallet_datum in self.wallets_data.items()
        ]

        for wallet_datum in result:
            if wallet_datum['min_coin_sent'] == float('inf'):
                wallet_datum['min_coin_sent'] = 0

            address = wallet_datum['address']
            if self.wallets_data[address]['eth_sent']['count'] > 0:
                wallet_datum['avg_coin_sent'] = self.wallets_data[address]['eth_sent']['sum'] / \
                                                self.wallets_data[address]['eth_sent']['count'],
            if self.wallets_data[address]['eth_received']['count'] > 0:
                wallet_datum['avg_coin_received'] = self.wallets_data[address]['eth_received']['sum'] / \
                                                self.wallets_data[address]['eth_received']['count'],

        return pd.DataFrame.from_records(result)


if __name__ == '__main__':
    df = pd.read_csv('../../data/0x38_wallets_pairs.csv')
    x_wallets = list(df['x'])
    y_wallets = list(df['y'])
    # end_block = 28705800
    end_block = 287058

    transactions_retriever = TransactionsRetriever(wallets_list=x_wallets, end_block=end_block, batch_size=10000,
                                                   chain_id='0x38')

    transactions_retriever.run()
    df_output = transactions_retriever.return_result()
    print(df_output.describe())