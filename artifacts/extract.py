import json

with open("./centralized_exchange_addresses.json", 'r') as f:
    data = json.load(f)
keys = list(data.keys())
print(keys)
print(len(keys))