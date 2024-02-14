
# import packages for this script
import sys
import os
import pandas as pd
import pickle

# -- Import functionalities/variables
import env_vars

# -- Load other scripts
from DeFi.Addresses import cex, dex, tokens
from Sources import Bitquery
from OnChain import LiquidityPool
from DeFi.Queries.uniswap_dex import bitquery_q2

p_network = 'ethereum'
p_dex = 'Uniswap'
wallet = 'gate_92Fe'
token = 'exrd'

p_ini_ts  = '2023-09-01T00:00:00'
p_end_ts  = '2023-10-01T00:00:00'

month = '0' + str(pd.to_datetime(p_ini_ts).month ) \
        if pd.to_datetime(p_ini_ts).month < 10 else pd.to_datetime(p_ini_ts).month
year = str(pd.to_datetime(p_ini_ts).year)
p_file_name = wallet + '_' + token + '_' + month + '_' + year

p_query = bitquery_q2
p_base_token = tokens[token]
p_cex_gate = cex[wallet]

# Tokens in env variables 
token_bqy = os.environ['TOKEN_BQ']

# Bitquery as source for OnChain data
bitquery = Bitquery(api_token=token_bqy)

# Instantiate OrderBooks Dataclass (a Subclass from @AbstractExchange)
lp = LiquidityPool(network_id=p_network, protocol_id=p_dex)

query_content = bitquery_q2

tokentransfers_data = lp.get_tokentransfers(source=bitquery,
                                            token_address=p_base_token, 
                                            sender_address=p_cex_gate,
                                            receiver_address=p_cex_gate,
                                            ini_ts=p_ini_ts, end_ts=p_end_ts,
                                            query_source='bitquery_queries', 
                                            query_content=query_content,
                                            resample=False)

#define a patch for the pickle file on your disk
pick_path = '/Users/francisco-munoz/github/OnChain-Data/Data/' + p_file_name

#convert the dictionary to pickle
with open (pick_path, 'wb') as pick:
    pickle.dump(tokentransfers_data, pick)

# Token inflow/outflow in CEX
# Time period is After the fork from PoW to PoS
# https://ethereum.org/en/history/#2023

# gate92fe - eXRD - jul.2023 [downloaded]
# gate92fe - eXRD - aug.2023 [downloaded]
# gate92fe - eXRD - sep.2023 [downloaded]

# gate_9Ca6 - lmwr - jul.2023 [downloaded]
# gate_9Ca6 - lmwr - aug.2023 [downloaded]
# gate_9Ca6 - lmwr - sep.2023 [downloaded]
