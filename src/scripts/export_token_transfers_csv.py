import csv
from csv import DictReader
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))

from databases.mongodb import MongoDB
from databases.postgresql import PostgresDB
from utils.format_utils import convert_tx_timestamp


def main():
    chain_id = '0x38'
    mongodb = MongoDB(chain_id=chain_id)
    postgres = PostgresDB()

    data = list()
    with open(f"../../data/{chain_id}_tokens_transfers.csv") as f:
        dict_reader = DictReader(f)
        list_of_dict = list(dict_reader)
        print(len(list_of_dict))

    tokens_addresses = set(datum['contract_address'] for datum in data)
    tokens_decimals = {row[0]: row[1] for row in postgres.get_decimals(chain_id=chain_id,
                                                                       token_addresses=list(tokens_addresses))}

    for datum in list_of_dict:
        contract_address = datum['contract_address']
        _decimals = tokens_decimals.get(contract_address, None)
        if _decimals:
            value = float(datum['value']) / (10**tokens_decimals[contract_address])
            block_timestamp = convert_tx_timestamp(datum['evt_block_time'], time_format='%Y-%m-%d %H:%M:%S.000 %Z')
            data.append(
                {
                    'contract_address': contract_address,
                    'transaction_hash': datum['evt_tx_hash'],
                    'log_index': int(datum['evt_index']),
                    'block_timestamp': block_timestamp,
                    'block_number': int(float(datum['evt_block_number'])),
                    'from_address': datum['from'],
                    'to_address': datum['to'],
                    'value': value
                }
            )

    _batch_size = 1000
    batches = [data[x:x+_batch_size] for x in range(0, len(data), _batch_size)]
    for i, batch in enumerate(batches):
        mongodb.export_transfer_events(data=batch)
        print(f"To batch {i} / {len(batches)}")
    print("done")


if __name__ == '__main__':
    main()