
# -- 
import requests
from gql.transport.requests import RequestsHTTPTransport
from gql import gql, Client

sqrt_price_x96 = 2350263685060625838582768195014121 # Consecuencia de operaciond el smartcontract

price_1 = (sqrt_price_x96/(2**96))**2 # usdc/weth
decimals_weth = 10e18
decimals_usdc = 10e6
decimals_wethusdc = int(decimals_weth/decimals_usdc)
price_wethusdc = (decimals_wethusdc)*(1/price_1)

