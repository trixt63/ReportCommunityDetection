from typing import List, Dict
from pymongo import MongoClient, UpdateOne

from config import MongoDBConfig

WALLETS_COL = 'lendingWallets'


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]
        self.lp_tokens_col = self._db['lpTokens']
        self.wallets_col = self._db[WALLETS_COL]
        self._groups_col = self._db['groups']
        self._users_col = self._db['users']

    #######################
    #  Generate dataset   #
    #######################
    def get_groups_by_num_wallets(self, chain_id, num_user_cond: dict or int, num_depo_cond: dict or int):
        pipeline = [
            {
                '$match': {
                    'num_user': num_user_cond,
                    'num_depo': num_depo_cond,
                    'Chain': chain_id
                }
            },
            {
                '$sort': {
                    'num_user': -1
                }
            }
        ]
        result = self._groups_col.aggregate(pipeline)
        return result

    def get_groups_by_twitter(self):
        pipeline = [
            {
                '$match': {
                    'twitter': {
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': '$twitter',
                    'numberOfAddresses': {
                        '$count': {}
                    },
                    'addresses': {
                        '$addToSet': '$address'
                    }
                }
            }, {
                '$match': {
                    'numberOfAddresses': {
                        '$gt': 1
                    }
                }
            }
        ]

        return self._users_col.aggregate(pipeline)

    def update_transactions(self, chain_id, data: List[Dict]):
        bulk_updates = [
            UpdateOne({'_id': datum['_id']}, {'$set': datum}, upsert=True)
            for datum in data
        ]
        self._db[f"{chain_id}_transactions"].bulk_write(bulk_updates)

    #######################
    #    Check dataset    #
    #######################
    def get_lending_wallet(self, address):
        return self._db['lendingWallets'].find_one({'_id': address})
        # return address

    def get_lp_deployer_wallet(self, address):
        return self._db['lpDeployers'].find_one({'_id': address})

    def get_lp_trader_wallet(self, address):
        return self._db['lpTraders'].find_one({'_id': address})

    #######################
    #      Analytics      #
    #######################
    def get_wallets(self, _filter, _projection):
        data = self.wallets_col.find(_filter, _projection)
        return data

    def count_wallets(self, filter):
        _count = self.wallets_col.count_documents(filter)
        return _count

    def count_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Count number of wallets of each project on each chain"""
        _filter = {f"{field_id}.{project_id}": {"$exists": 1}}
        _projection = {f"{field_id}.{project_id}": 1}
        deployments = self.wallets_col.find(_filter, _projection)
        _count = 0
        for _depl in deployments:
            for project in _depl[field_id][project_id]:
                if project['chainId'] == chain_id:
                    _count += 1
                    continue
        return _count

    def count_exchange_deposit_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Each CEX project stores a list of chain_ids, instead a list of objects like other type of project,
        so I need a separate function to handle this"""
        _filter = {f"{field_id}.{project_id}": chain_id}
        _count = self.wallets_col.count_documents(_filter)
        return _count

    def get_number_lps_by_balances_range(self, chain_id, upper=0, lower=0):
        _filter = {'chainId': chain_id}
        _balance_filter = {}
        if upper:
            _balance_filter['$lt'] = upper / 2
        if lower:
            _balance_filter['$gte'] = lower/ 2
        if _balance_filter:
            _filter.update({"pairBalancesInUSD.token0": _balance_filter})
        return self.lp_tokens_col.count_documents(filter=_filter)

    def get_duplicated_lp_deployer(self, pancake=0, uniswap=0, spooky=0):
        filter_ = {'tags': 'lp_owner', 
                   'ownedLPs.pancakeswap': {'$exists': pancake},
                   'ownedLPs.spookyswap': {'$exists': spooky},
                   'ownedLPs.uniswap': {'$exists': uniswap}
                   }
        return self.wallets_col.find(filter_)
    
    def get_pair_addresses(self, chain_id, lp_address):
        filter_ = {'_id': f"{chain_id}_{lp_address}"}
        lp_contract = self.lp_tokens_col.find_one(filter_)
        token0 = lp_contract.get('token0')
        token1 = lp_contract.get('token1')
        return token0, token1
