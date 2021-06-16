package com.jvo.dto

case class ThresholdEvent(eventId: String,
                          individualTaxNumber: String,
                          amount: BigDecimal,
                          fromDateTime: String,
                          toDateTime: String)