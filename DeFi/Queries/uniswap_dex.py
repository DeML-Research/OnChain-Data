# ---------------------------------------------------------------------------------------- -------------- -- #
# ---------------------------------------------------------------------------------------- UNISWAP TRADES -- #
# ---------------------------------------------------------------------------------------- -------------- -- #

# ------------------------------------------------------------------------------------ EVM Uniswap Trades -- #
# ------------------------------------------------------------------------------------ ------------------ -- #

bitquery_q1 = """ 
query ($protocol_id: String!,
       $ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime,
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
query ($ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime, 
       $token_address: String, $receiver_address: String!, $sender_address: String!) {
  ethereum(network: ethereum) {
    transfers(
      time: {since: $ini_ts, till: $end_ts}
      currency: {is: $token_address}
      any: [{sender: {in: [$sender_address]}}, {receiver: {in: [$receiver_address]}}]
      amount: {gt: 0}
    ) {
      sender {
        address
        annotation
      }
      receiver {
        address
        annotation
      }
      amount
      count
      countBigInt
      entityId
      gasValue
      external
      success
      block {
        timestamp {
          iso8601
        }
      }
    }
  }
}
"""

# -------------------------------------------------------------------------------- EVM Uniswap Top Wallets -- #
# -------------------------------------------------------------------------------- ----------------------- -- #

bitquery_q3 = """
query ($token_address: String!, $limit: Int, $ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime) {
            ethereum(network: ethereum) {
                transfers(
                    currency: {is: $token_address}
                    date: {since: $ini_ts, till: $end_ts}
                    height: {gt: 0}
                    amount: {gt: 0}
                    options: {desc: "amount", limit: $limit, offset: 10}){
                    sender {
                        address
                        annotation
                    }
                    receiver {
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

# ------------------------------------------------------------------------------- Uniswap v3 SmartContract -- #
# ------------------------------------------------------------------------------- ------------------------ -- #

bitquery_q4 = """
query ($ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime, $smartcontract_address: String!) {
  ethereum(network: ethereum) {
    smartContractCalls(
      smartContractAddress: {is: $smartcontract_address}
      options: {limit: 67}
    ) { 	
      block(time: {since: $ini_ts, till: $end_ts}) {
        height
        timestamp {
          iso8601
        }
      }
      arguments {
        argument
        index
        value
        argumentType
      }
      callDepth
      smartContractMethod {
        name
        signature
        signatureHash
      }
      gasValue
      external
      smartContract {
        contractType
        protocolType
      }
      caller {
        address
        smartContract {
          contractType
        }
        annotation
      }
      count
    }
    smartContractEvents {
      arguments {
        argument
        argumentType
        index
        value
      }
      smartContractEvent {
        name
        signature
        signatureHash
      }
      smartContract {
        address {
          address
          annotation
        }
        contractType
        protocolType
        currency {
          name
        }
      }
    }
  }
}
"""

# ------------------------------------------------------------------------------- Uniswap v3 SmartContract -- #
# ------------------------------------------------------------------------------- ------------------------ -- #

bitquery_q5 = """
query ($ini_ts: ISO8601DateTime, $end_ts: ISO8601DateTime, $smartcontract_address: String!) {
  ethereum(network: ethereum) {
    smartContractCalls(smartContractAddress: {is: $smartcontract_address}) {
      block(time: {since: $ini_ts, till: $end_ts}) {
        timestamp {
          iso8601
        }
        height
      }
      amount
      gasValue
      external
      count
      arguments {
        argument
        index
        value
        argumentType
      }
      smartContractMethod {
        name
        signature
        signatureHash
      }
      caller {
        address
        annotation
        smartContract {
          contractType
          protocolType
          currency {
            name
            symbol
          }
        }
      }
      
    }
  }
}
"""