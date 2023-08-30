import copy
import csv
import networkx as nx
from csv import DictReader
from typing import Dict, List
from pandas import read_csv, DataFrame

from constants.network_constants import Chains
from databases.blockchain_etl import BlockchainETL
from multithread_processing.base_job import BaseJob
from utils.logger_utils import get_logger

logger = get_logger('Grid Search Subgraph Radius Job')


class GridSearchSubgraphRadiusJob(BaseJob):
    def __init__(self, chain_id, start_block, end_block, radius,
                 batch_size=10000, max_workers=8):
        self.chain_id = chain_id
        self.start_block = start_block
        self.end_block = end_block
        self.work_iterable = list(range(start_block, end_block+1))

        if chain_id == '0x38':
            self.blockchain_etl = BlockchainETL()
        else:
            _chain_name = Chains.names[chain_id]
            self.blockchain_etl = BlockchainETL(db_prefix=_chain_name)

        super().__init__(work_iterable=self.work_iterable,
                         batch_size=batch_size,
                         max_workers=max_workers)

        self.radius = radius

        self.wallets_pairs_dict: List[Dict] = list()
        self.wallets_pairs_df: DataFrame = DataFrame()

    def _start(self):
        with open(f'../../data/{self.chain_id}_wallets_pairs.csv') as f:
            dict_reader = DictReader(f)
            self.wallets_pairs_dict = list(dict_reader)

        self.wallets_pairs_df = read_csv(f'../../data/{self.chain_id}_wallets_pairs.csv', index_col=0)
        self.wallets_pairs_df['has_y'] = [0] * len(self.wallets_pairs_dict)
        self.wallets_pairs_df['first_block_number'] = [0] * len(self.wallets_pairs_dict)

        self.x_wallets = set(self.wallets_pairs_df['x'])

    def _execute_batch(self, works):
        _from_block = works[0]
        _to_block = works[-1]
        tx_graph = nx.Graph()

        wallets_checked = set()
        wallets_to_check = self.x_wallets
        for i in range(self.radius):
            _transactions = self.blockchain_etl.get_transactions_related_to_addresses(addresses=list(wallets_to_check),
                                                                                      from_block=_from_block,
                                                                                      to_block=_to_block)
            for tx in _transactions:
                if tx['to_address']:
                    tx_graph.add_edge(tx['from_address'], tx['to_address'], block_number=tx['block_number'])

            for _idx in self.wallets_pairs_df.index:
                if not self.wallets_pairs_df['has_y'][_idx]:
                    x_addr = self.wallets_pairs_df.loc[_idx, 'x']
                    y_addr = self.wallets_pairs_df.loc[_idx, 'y']
                    if tx_graph.has_edge(x_addr, y_addr):
                        self.wallets_pairs_df.loc[_idx, 'has_y'] = 1
                        self.wallets_pairs_df.loc[_idx, 'first_block_number'] = \
                            tx_graph.get_edge_data(x_addr, y_addr).get('block_number')

            # for next iteration
            wallets_checked = wallets_checked.union(wallets_to_check)
            wallets_to_check = set(tx_graph.nodes()).difference(wallets_checked)

        logger.info(f"Finish {_from_block} to {_to_block} / {self.end_block}. "
                    f"Progress {(_to_block - self.start_block) / (self.end_block - self.start_block) * 100}%")

    def _end(self):
        super()._end()
        self.wallets_pairs_df.to_csv(f'../../data/gridsearch_subgraph_{self.chain_id}_radius_{self.radius}.csv')


if __name__ == '__main__':
    job = GridSearchSubgraphRadiusJob(
        chain_id='0x38',
        start_block=26925539,
        end_block=28195220,
        radius=2,
        batch_size=10000,
        max_workers=8
    )
    job.run()
