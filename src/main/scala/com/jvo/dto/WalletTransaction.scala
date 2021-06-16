package com.jvo.dto

import java.sql.Timestamp

case class WalletTransaction(customerId: String,
                             transactionId: String,
                             transactionDate: Timestamp,
                             transactionType: String,
                             deviceType: String,
                             isFinal: Boolean,
                             gameRoundReference: String,
                             amount: BigDecimal,
                             providerName: String)
