
# import packages for this script
import sys
import os
import pandas as pd

# Load other scripts
from DeFi.Addresses import cex, dex, tokens
from Sources import Bitquery
from OnChain import LiquidityPool
from DeFi.Queries import uniswap_dex

# Write and Load environment variables
import env_vars

# Parameters for the example
p_ini_ts = '2023-01-01T00:00:00'
p_end_ts = '2023-01-13T00:00:00'
p_network = 'ethereum'
p_dex = 'Uniswap'
p_base_token = tokens['ant']
p_quote_token = tokens['usdt']
p_dex_uniswap = dex['ant_usdt']

# Tokens in env variables 
token_bqy = os.environ['TOKEN_BQ']

# Bitquery as source for OnChain data
bitquery = Bitquery(api_token=token_bqy)

# Instantiate OrderBooks Dataclass (a Subclass from @AbstractExchange)
lp = LiquidityPool(network_id=p_network, protocol_id=p_dex)

# ----------------------------------------------------------------- TOKEN TRANSFERS -- #
# ----------------------------------------------------------------- --------------- -- #

query_content = uniswap_dex.bitquery_q2

tokentransfers_data = lp.get_tokentransfers(source=bitquery,
                                            token_address=p_base_token, 
                                            sender_address=p_dex_uniswap,
                                            receiver_address=p_dex_uniswap,
                                            ini_ts=p_ini_ts, end_ts=p_end_ts,
                                            query_source='bitquery_queries', 
                                            query_content=query_content,
                                            resample=False)

# Pump And Dump algorithm for Dex-based TOB prices
# 

# ---------------------------------------------------------------------- DEX TRADES -- #
# ---------------------------------------------------------------------- ---------- -- #

# graphQL query providing raw query content
query_content = uniswap_dex.bitquery_q1

# Get data providing raw query content
dextrades_data = lp.get_dextrades(source=bitquery,
                                  base_token=p_base_token, 
                                  quote_token=p_quote_token,
                                  ini_ts=p_ini_ts, end_ts=p_end_ts,
                                  query_source='bitquery_queries', 
                                  query_content=query_content,
                                  resample=False)

# Numeralia of the collected data
total_dextradess = len(dextrades_data)
first_dt = dextrades_data.iloc[0]
last_dt = dextrades_data.iloc[-1]

# Save it in .parquet format
# lp.write_data(object_data=dextrades_data, input_format='dataframe', output_format='parquet',
#               file_name='uniswap_wbtcusdt_dextrades_jan2023',
#               file_dir='/Users/francisco-munoz/Documents/gitlab/')

dextrades_data.to_csv('uniswap_wbtcusdt_dextrades_jan2023.csv')
# graphQL query providing raw query content

# ----------------------------------------------------------------------TOP WALLETS -- #
# --------------------------------------------------------------------- ----------- -- #

# graphQL query providing raw query content
query_content = uniswap_dex.bitquery_q3

topwallets_data = lp.get_topwallets(source=bitquery,
                                    token_address=p_base_token,
                                    ini_ts=p_ini_ts, end_ts=p_end_ts,
                                    query_source='bitquery_queries', 
                                    query_content=query_content,
                                    resample=False)

pd.DataFrame(topwallets_data[1])
