import os
import sys
sys.path.append(os.path.dirname(sys.path[0]))

import pandas as pd
from typing import List, Dict

from src.scripts.transactions_features import TransactionsFeatures


def main():
    chain_id = '0x38'
    df = pd.read_csv(f'../../data/{chain_id}_wallets_pairs.csv')

    # encode transactions feature
    print("Encode Transactions Features for x")
    x_wallets = list(df['x'])
    x_transactions_features = _encode_transactions_feature(x_wallets, chain_id=chain_id)
    x_transactions_features.to_csv(f'../../data/transactions_features_{chain_id}_x.csv')
    print("Encode Transactions Features for y")
    y_wallets = list(df['y'])
    y_transactions_features = _encode_transactions_feature(y_wallets, chain_id=chain_id)
    y_transactions_features.to_csv(f'../../data/transactions_features_{chain_id}_y.csv')


def _encode_transactions_feature(wallets_list, chain_id) -> pd.DataFrame:
    transactions_retriever = TransactionsFeatures(wallets_list=wallets_list,
                                                  batch_size=1000,
                                                  max_workers=8,
                                                  chain_id=chain_id)
    transactions_retriever.run()
    return transactions_retriever.return_result()


if __name__ == '__main__':
    main()
