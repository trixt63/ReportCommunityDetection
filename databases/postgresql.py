from typing import List
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

    def get_decimals(self, chain_id: str, token_addresses=[]):
        if token_addresses:
            query = text(f"""
                SELECT * FROM chain_{chain_id}.token_decimals
                WHERE address = ANY (ARRAY{token_addresses})
            """)
        else:
            query = text(f"""
                SELECT * FROM chain_{chain_id}.token_decimals
            """)
        result = self.session.execute(query)
        self.session.commit()
        return result


if __name__ == '__main__':
    postgres = PostgresDB()
    contract_addr = '0x0dfdd7d67c0d2f2db518b7cbfdf66c038d1f0040'