from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from config import BlockchainETLConfig
from constants.blockchain_etl_constants import BlockchainETLCollections, BlockchainETLIndexes
from constants.time_constants import TimeConstants
from utils.logger_utils import get_logger
from utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('Blockchain ETL')


class BlockchainETL:
    def __init__(self, connection_url=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = BlockchainETLConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        if db_prefix:
            db_name = db_prefix + "_" + BlockchainETLConfig.DATABASE
        else:
            db_name = BlockchainETLConfig.DATABASE

        self.mongo_db = self.connection[db_name]

        self.block_collection = self.mongo_db[BlockchainETLCollections.blocks]
        self.transaction_collection = self.mongo_db[BlockchainETLCollections.transactions]
        self.collector_collection = self.mongo_db[BlockchainETLCollections.collectors]

    # @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_relate_to_address(self, address, from_block, to_block):
        filter_ = {
            "block_number": {"$gte": from_block, "$lt": to_block},
            "$or": [
                {"from_address": address},
                {"to_address": address}
            ]
        }
        projection = ['to_address', 'value', 'block_number', 'block_timestamp']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_to_transactions(self, from_address, from_block, to_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lt": to_block}},
                {"from_address": from_address}
            ]
        }
        projection = ['to_address', 'value', 'block_number', 'block_timestamp']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    # @sync_log_time_exe(tag=TimeExeTag.database)
    def get_from_transactions(self, to_address, from_block, to_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lt": to_block}},
                {"to_address": to_address}
            ]
        }
        projection = ['from_address']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    # @sync_log_time_exe(tag=TimeExeTag.database)
    # def get_transactions_to_addresses(self, to_addresses, from_block, to_block):
    #     filter_ = {
    #         "$and": [
    #             {"block_number": {"$gte": from_block, "$lte": to_block}},
    #             {"to_address": {"$in": [address.lower() for address in to_addresses]}}
    #         ]
    #     }
    #     projection = ['from_address']
    #     cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
    #     return cursor
    #
    # @sync_log_time_exe(tag=TimeExeTag.database)
    # def get_transactions_from_addresses(self, from_addresses, from_block, to_block):
    #     filter_ = {
    #         "$and": [
    #             {"block_number": {"$gte": from_block, "$lte": to_block}},
    #             {"from_address": {"$in": [address.lower() for address in from_addresses]}}
    #         ]
    #     }
    #     projection = ['to_address']
    #     cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
    #     return cursor
    #
    # def get_native_transfer_txs(self, from_block, to_block):
    #     filter_ = {
    #         "$and": [
    #             {"block_number": {"$gte": from_block, "$lte": to_block}},
    #             {"input": "0x"},
    #             {"value": {"$ne": "0"}},
    #             {"receipt_status": 1}
    #         ]
    #     }
    #     projection = ['from_address', 'to_address', 'value', 'block_timestamp', 'hash']
    #     cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
    #     return cursor
    #
    # def get_the_first_tx(self, address):
    #     filter_ = {
    #         "$or": [
    #             {"from_address": address},
    #             {"to_address": address}
    #         ]
    #     }
    #     projection = ['block_timestamp']
    #     cursor = self.transaction_collection.find(filter_, projection=projection).sort('block_number').limit(1)
    #     return list(cursor)
