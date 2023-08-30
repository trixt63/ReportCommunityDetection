import pandas as pd
import random
from typing import Dict, List, Tuple
from itertools import combinations

from databases.mongodb import MongoDB

mongodb = MongoDB()


def generate_wallets_pairs(chain_id):
    groups = mongodb.get_groups_by_num_wallets(chain_id=chain_id,
                                               num_user_cond={'$gt': 1, '$lte': 4},
                                               num_depo_cond=1)
    wallets_pairs: List[Tuple] = list()

    for group in groups:
        users = list(group['user_wallets'])
        users_pairs = list(combinations(users, 2))
        wallets_pairs.extend(users_pairs)

    return wallets_pairs


def generate_dataset(chain_id):
    wallet_pairs = generate_wallets_pairs(chain_id)
    _df = pd.DataFrame.from_records([{'x': pair[0], 'y': pair[1]} for pair in wallet_pairs])
    _df.to_csv(f'../../data/{chain_id}_wallets_pairs.csv')
    return _df


def generate_wallets_pairs_social():
    data = mongodb.get_groups_by_twitter()
    wallets_pairs = [datum['addresses'] for datum in data]
    _df = pd.DataFrame.from_records({'x': pair[0], 'y': pair[1]} for pair in wallets_pairs)
    _df.to_csv('../../data/social_wallets_pairs.csv')


if __name__ == '__main__':
    generate_dataset('0x38')
    # generate_dataset('0x31')
    # generate_wallets_pairs_social()
