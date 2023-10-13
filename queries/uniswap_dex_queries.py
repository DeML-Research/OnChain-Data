# ---------------------------------------------------------------------------------------- -------------- -- #
# ---------------------------------------------------------------------------------------- UNISWAP TRADES -- #
# ---------------------------------------------------------------------------------------- -------------- -- #

# ------------------------------------------------------------------------------------ EVM Uniswap Trades -- #
# ------------------------------------------------------------------------------------ ------------------ -- #

bitquery_q1 = """ 
query ($protocol_id: String!, $ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime,
       $base_address: String, $quote_address: String) {
  ethereum(network: ethereum) {
    dexTrades(
      exchangeName: {in: [$protocol_id]}
      baseCurrency: {is: $base_address}
      quoteCurrency: {is: $quote_address}
      options: {asc: ["block.timestamp.iso8601"]}
    ) {
      block(time: {since: $ini_ts, till: $end_ts}) {
        height
        timestamp {iso8601}
      }
      tradeIndex
      side
      price
      quoteAmount
      baseAmount
      buyCurrency {symbol}
      sellCurrency {symbol}
      maker {address}
      taker {address}
      transaction {
        gas
        gasPrice
        gasValue
        hash
        index
        nonce
      }
      date {date}
    }
  }
}
"""

# ---------------------------------------------------------------------------- EVM Uniswap Token Transfers -- #
# ---------------------------------------------------------------------------- --------------------------- -- #

bitquery_q2 = """
query ($network: EthereumNetwork!, $address: String, $from: ISO8601DateTime, $till: ISO8601DateTime) {
        ethereum(network: $network) {
            out: transfers(
            currency: {is: "ETH"}
            date: {since: $from, till: $till}
            options: {asc: "out"}
            any: [{sender: {is: $address}}, {receiver: {is: $address}}]
            time: {}
            ) {
            in: amount(receiver: {is: $address})
            out: amount(sender: {is: $address})
            }
        }
    }
"""

# -------------------------------------------------------------------------------- EVM Uniswap Top Wallets -- #
# -------------------------------------------------------------------------------- ----------------------- -- #

bitquery_q3 = """
query ($network: EthereumNetwork!, $token: String!, $limit: Int,
                $ts_start: ISO8601DateTime, $ts_end: ISO8601DateTime) {
            ethereum(network: $network) {
                transfers(
                    currency: {is: $token}
                    date: {since: $ts_start, till: $ts_end}
                    height: {gt: 0}
                    amount: {gt: 0}
                    options: {desc: "amount", limit: $limit, offset: 10}){
                    sender {
                        address
                        annotation
                    }
                    currency {
                        symbol
                    }
                    amount
                    count
                    receiver_count: count(uniq: receivers)
                    }
                }
            }
"""
