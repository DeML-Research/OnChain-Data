query MyQuery {
  swaps(
    where: {timestamp_gte: 1688018401, pool_: {id: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"}}
  ) {
    id
    amount0
    amount1
    amountUSD
    timestamp
  }
  pool(id: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640") {
    sqrtPrice
  }
}