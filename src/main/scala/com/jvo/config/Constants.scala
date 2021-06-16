package com.jvo.config

import java.time.format.DateTimeFormatter

object Constants {

  final val BetTransaction = "bet"
  final val WinTransaction = "win"
  final val DepositTransaction = "deposit"
  final val WithdrawalTransaction = "withdrawal"

  //Cassandra columns
  final val CustomerIdColumn = "customer_id"
  final val IndividualTaxNumberColumn = "individual_tax_number"
  final val BetsLimitColumn = "bets_limit"
  final val BetsTimeWindowColumn = "bets_time_window"
  final val DepositLimitColumn = "deposits_limit"
  final val DepositTimeWindowColumn = "deposits_time_window"
  final val LossesLimitColumn = "losses_limit"
  final val LossesTimeWindowColumn = "losses_time_window"

  final val GameRoundReferenceColumn = "game_round_reference"
  final val TransactionDateColumn = "transaction_date"
  final val ProviderNameColumn = "provider_name"
  final val ResultDateColumn = "result_date"
  final val GameRoundAmountColumn = "game_round_amount"
  final val AmountColumn = "amount"
  final val ResultTypeColumn = "result_type"
  final val LossAmountColumn = "loss_amount"
  final val FromDateTimeColumn = "from_date_time"
  final val ToDateTimeColumn = "to_date_time"
  final val EventIdColumn = "event_id"
  final val LatestBigLossTimestamp = "latest_big_loss_timestamp"
  final val TimestampCassandraQueryFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss+0000")
  final val EventDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  final val GameRoundWin = "win"
  final val GameRoundLoss = "loss"

  final val DefaultThresholdWindowHours: Int = 12
  final val DefaultLossThreshold: Double = 2000

}