import time
from pprint import pprint  # For formatted dictionary printing
from whalealert.whalealert import WhaleAlert
whale = WhaleAlert()

# Specify a single transaction from the last 10 minutes
start_time = int(time.time() - 600)
api_key = 'P2mb5TD3bZPQP1bWPTmMCkjDBM8EJGft'
transaction_count_limit = 1

success, transactions, status = whale.get_transactions(start_time, api_key=api_key, limit=transaction_count_limit)
print(success)
pprint(transactions)