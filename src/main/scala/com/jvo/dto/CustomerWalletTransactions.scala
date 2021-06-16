package com.jvo.dto

case class CustomerWalletTransactions(customerId: String, transactions: Seq[WalletTransaction])
