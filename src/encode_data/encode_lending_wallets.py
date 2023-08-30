import pandas as pd
from multithread_processing.base_job import BaseJob
from typing import Dict, List


class EncodeLendingWallets(BaseJob):
    def __init__(self, wallets_list: list, filename: str,
                 max_workers=16, batch_size=100):
        self.filename = filename
        self.onehot_mapper = {'aave': 0, 'venus': 1, 'trava': 2, 'cream': 3, 'valas': 4, 'compound': 5, 'geist': 6}  # bsc: venus, trava, cream, valas

        self.wallets_encoded: Dict[str: List] = dict()

        super().__init__(work_iterable=wallets_list,
                         max_workers=max_workers,
                         batch_size=batch_size)

    def _execute_batch(self, works):
        for wallet in works:
            address = wallet['address']
            pools = wallet['lendingPools']
            onehot_vector = [0] * 7
            for pool, onehot_position in self.onehot_mapper.items():
                if pool in pools:
                    onehot_vector[onehot_position] = 1
            self.wallets_encoded[address] = onehot_vector

    def _end(self):
        super()._end()
        # save to csv
        df = pd.DataFrame.from_dict(self.wallets_encoded,
                                    orient='index',
                                    columns=['aave', 'venus', 'trava', 'cream', 'valas', 'compound', 'geist'])
        df.to_csv(self.filename)

