# -- Load Packages
import sys
import os

# -- Load Addresses, Sourcing and Data objects
from Addresses import cex, dex, tokens
from Sources import Bitquery
from OnChain import LiquidityPool
from queries.uniswap_dex_queries import bitquery_q1

# Parameters for the example
p_ini_ts = '2023-08-01 00:00:00'
p_end_ts = '2023-08-05 00:00:00'
p_network = 'ethereum'
p_dex = 'Uniswap'
p_base_token = tokens['weth']
p_quote_token = tokens['usdt']

# Tokens in env variables 
token_bqy = "BQYHgFw9VGtoCALvHp6y0DvIpgu0Ej6h"

# Bitquery as source for OnChain data
bitquery = Bitquery(api_token=token_bqy)

# Instantiate OrderBooks Dataclass (a Subclass from @AbstractExchange)
lp = LiquidityPool(network_id=p_network, protocol_id=p_dex)

# graphQL query, method 1 : Provide raw query content
query_content = bitquery_q1

# Get data providing raw query content
dextrades_data = lp.get_dextrades(source=bitquery, base_token=p_base_token, quote_token=p_quote_token,
                                  ini_ts=p_ini_ts, end_ts=p_end_ts,
                                  query_source='bitquery_queries', query_content=query_content,
                                  resample=True, period='1T')

# Numeralia of the collected data
total_dextradess = len(dextrades_data)
first_dt = dextrades_data.iloc[0]
last_dt = dextrades_data.iloc[-1]

# Save it in .parquet format
lp.write_data(object_data=dextrades_data, input_format='dataframe', output_format='csv',
              file_name='uniswap_wethusdt_dextrades',
              file_dir='/Users/francisco-munoz/Documents/')
