package com.jvo.dto

case class IndividualLimits(individualTaxNumber: String,
                            betsLimit: Option[Double],
                            betsTimeWindow: Option[Int],
                            depositLimit: Option[Double],
                            depositTimeWindow: Option[Int],
                            lossesLimit: Option[Double],
                            lossesTimeWindow: Option[Int])

