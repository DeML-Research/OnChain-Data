
import pandas as pd
import pickle

all_transfers = []

file_name = '/Users/francisco-munoz/github/OnChain-Data/Data/'+ 'gate_92fe_exrd_' + 'sep23'
with open(file_name, 'rb') as f:
    wallet_data = pickle.load(f)

len(all_transfers)
type(wallet_data)

for i_transfer in wallet_data:
    i_tx_data = i_transfer['data']['ethereum']['transfers']
    for i_tx in i_tx_data:
        dc_data = {
            'sender_address': i_tx['sender']['address'],
            'receiver_address': i_tx['receiver']['address'],
            'amount': i_tx['amount'],
            'count': i_tx['count'],
            'gasValue': i_tx['gasValue'],
            'external': i_tx['external'],
            'success': i_tx['success'],
            'timestamp': i_tx['block']['timestamp']['iso8601']
        }
        all_transfers.append(dc_data)


df_transfers = pd.read_csv('/Users/francisco-munoz/github/OnChain-Data/Data/'+
                                    'token_transfers_gate_9Ca6_exrd_jul_aug.csv', header=0,
                                    index_col=0)
