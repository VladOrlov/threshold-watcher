package com.jvo.service

import com.jvo.config.Constants._
import com.jvo.dto.{ThresholdEvent, CheckIndividualThresholds, IndividualLimits, IndividualPlayResult, LossCheckEvent}

import java.time.LocalDateTime

class ThresholdsChecker(databaseService: DatabaseService) extends Serializable {

  def processGameRoundFinalTransaction(lossCheckEvent: CheckIndividualThresholds): Option[ThresholdEvent] = {

    val maybeLimits: Option[IndividualLimits] = databaseService.getIndividualLimits(lossCheckEvent.individualTaxNumber)
    val lossesCutOffDateTime = getIndividualLossesCutOffDateTime(lossCheckEvent, maybeLimits)

    databaseService
      .getCustomerPlayResultForPeriod(lossCheckEvent.individualTaxNumber, lossesCutOffDateTime)
      .filter(individualPlayResult => isCustomerLossHitThreshold(individualPlayResult, maybeLimits))
      .map(processToBigLossEvent)
  }

  private def processToBigLossEvent(result: IndividualPlayResult) = {
    databaseService.saveBigLossEvent(result)
    ThresholdEvent(
      eventId = result.id,
      individualTaxNumber = result.individualTaxNumber,
      amount = result.lossAmount,
      fromDateTime = result.fromDateTime.toLocalDateTime.format(EventDateFormatter),
      toDateTime = result.toDateTime.toLocalDateTime.format(EventDateFormatter)
    )
  }

  private def getIndividualLossesCutOffDateTime(lossCheckEvent: CheckIndividualThresholds, maybeLimits: Option[IndividualLimits]) = {
    maybeLimits
      .flatMap(individualLimits => individualLimits.lossesTimeWindow
        .map(windowInHours => lossCheckEvent.checkDateTime.minusHours(windowInHours)))
      .map(lossesCutOffDateTime =>
        getLatestDate(lossesCutOffDateTime, databaseService.getCustomerLatestBigLossEventDate(lossCheckEvent.individualTaxNumber)))
      .getOrElse(getDefaultCutOffTime(lossCheckEvent.checkDateTime))
  }

  private def getDefaultCutOffTime(checkDateTime: LocalDateTime) = {
    checkDateTime.minusHours(DefaultThresholdWindowHours)
  }

  private def getLatestDate(date1: LocalDateTime, maybeDate2: Option[LocalDateTime]) = {
    maybeDate2
      .map(date2 => if (date1.isBefore(date2)) date2 else date1)
      .getOrElse(date1)
  }

  private def isCustomerLossHitThreshold(playResult: IndividualPlayResult, maybeLimits: Option[IndividualLimits]): Boolean = {
    val individualLossesLimit: Double = maybeLimits
      .flatMap(_.lossesLimit)
      .getOrElse(DefaultLossThreshold)

    playResult.lossAmount < 0 && playResult.lossAmount.abs > individualLossesLimit
  }

  //  private def getGameRoundResultsForCustomerFinalTransactions(customerWalletTransaction: CustomerWalletTransactions) = {
  //    customerWalletTransaction.transactions
  //      .flatMap(walletTransaction => databaseService
  //        .getCustomerGameRoundTransactions(walletTransaction.customerId, walletTransaction.gameRoundReference))
  //  }
  //
  //  private def getCutOffDateFromRoundResults(checkDate: LocalDateTime, individualLimits: IndividualLimits): LocalDateTime = {
  //
  //    gameRoundResults.maxBy(_.resultDate.toInstant)
  //      .resultDate.toLocalDateTime
  //      .minusHours(DefaultThresholdWindowHours)
  //  }
}

object ThresholdsChecker {
  def apply(databaseService: DatabaseService): ThresholdsChecker = new ThresholdsChecker(databaseService)
}