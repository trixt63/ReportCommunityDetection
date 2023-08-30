from typing import List, Dict
from pymongo import MongoClient, UpdateOne

from config import MongoDBEntityConfig
from constants.mongodb_entity_constants import LPConstants
from utils.logger_utils import get_logger

logger = get_logger('MongoDB Entity')


class MongoDBEntity:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBEntityConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBEntityConfig.DATABASE]
        self._config_col = self._db['configs']
        self._multichain_wallets_col = self._db['multichain_wallets']
        self._smart_contracts_col = self._db['smart_contracts']

    def get_meme_coins(self, chain_id: str, projection: {}):
        return self._smart_contracts_col.find(filter={'chainId': chain_id, 'categories': "Meme"},
                                              projection=projection)

    def get_stable_coins(self, chain_id: str, projection: {}):
        return self._smart_contracts_col.find(filter={'chainId': chain_id, 'categories': "Stablecoins"},
                                              projection=projection)

    def get_contracts(self, filter_={}, projection={}):
        return self._smart_contracts_col.find(filter=filter_, projection=projection)

    def get_token(self, chain_id, address):
        _filter = {'_id': f"{chain_id}_{address}"}
        return self._smart_contracts_col.find_one(_filter)

    def get_token_symbol(self, chain_id, address):
        token = self.get_token(chain_id, address)
        return token.get('symbol', None)
