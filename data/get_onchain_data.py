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

p_ini_ts = '2022-10-01T00:00:00'
p_end_ts = '2023-10-01T23:59:59'
p_network = 'ethereum'
p_dexes = []
p_cexes = []


for i in range(0, 76):
    print(i)
    data_antusdt['ethereum']['smartContractCalls'][i]['smartContractMethod']['name']

# Caller of the smart contract (Uniswap v4 LP)
data['ethereum']['smartContractCalls'][145]['caller']['address']

# A particular call to the smartcontract
data['ethereum']['smartContractCalls'][145].keys()

# Index and value to update
data['ethereum']['smartContractCalls'][145]['smartContractMethod']
data['ethereum']['smartContractCalls'][145]['arguments'][0]['index']
data['ethereum']['smartContractCalls'][145]['arguments'][0]['value']

import json

with open("uniswapv3_antusdt_dex.json", "w") as file:
    json.dump(data_antusdt, file)