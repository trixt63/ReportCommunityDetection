import pandas as pd
from multithread_processing.base_job import BaseJob

from databases.mongodb import MongoDB
from databases.mongodb_entity import MongoDBEntity
from src.scripts.transactions_retriever import TransactionsRetriever


def main():
    df = pd.read_csv('../../data/0x38_wallets_pairs.csv')
    x_wallets = list(df['x'])
    y_wallets = list(df['y'])
    # end_block = 28705800
    end_block = 28705

    transactions_retriever = TransactionsRetriever(wallets_list=x_wallets,
                                                   end_block=end_block,
                                                   batch_size=100,
                                                   max_workers=8,
                                                   chain_id='0x38')

    transactions_retriever.run()
    df_output = transactions_retriever.return_result()
    df_output.describe()
    df_output.to_csv('../../data/0x38/x_transactions_features.csv')


if __name__ == '__main__':
    main()