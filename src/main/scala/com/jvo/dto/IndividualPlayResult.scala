package com.jvo.dto

import java.sql.Timestamp
import java.util.UUID

case class IndividualPlayResult(id: String = UUID.randomUUID().toString,
                                individualTaxNumber: String,
                                lossAmount: BigDecimal,
                                fromDateTime: Timestamp,
                                toDateTime: Timestamp)