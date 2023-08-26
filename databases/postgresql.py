from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from config import PostgresDBConfig
from constants.network_constants import NATIVE_TOKEN
from constants.postgres_constants import TransferEvent, WALLET_TABLES, TF_EVENTS_VIEW, AmountInOut
from utils.logger_utils import get_logger
from utils.time_execute_decorator import TimeExeTag, sync_log_time_exe

logger = get_logger('PostgreSQL')


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)

        # Create a session to manage the database transactions
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()

    def get_decimals(self, chain_id, token_addresses):
        query = text(f"""
            SELECT * FROM chain_{chain_id}.token_decimals
            WHERE address = ANY (ARRAY{token_addresses})
        """)
        result = self.session.execute(query)
        self.session.commit()
        return result


if __name__ == '__main__':
    postgres = PostgresDB()
    token_addresses = ['0x55d398326f99059ff775485246999027b3197955',
                       '0xe9e7cea3dedca5984780bafc599bd69add087d56',
                       '0xfa82075a6d8f85be9146e64e0f02baa849f8e8fb']
    _decimals = postgres.get_decimals(chain_id='0x38', token_addresses=token_addresses).all()
    coins_decimals = {row[0]: row[1] for row in _decimals}
    print(coins_decimals)