package com.jvo.service

import com.datastax.oss.driver.api.core.cql.{ResultSet, Row}
import com.datastax.spark.connector.cql.CassandraConnector
import com.jvo.config.Constants._
import com.jvo.config.dto.CassandraConfig
import com.jvo.dto.{IndividualPlayResult, IndividualLimits}
import org.apache.spark.SparkConf

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

class DatabaseService(val connector: CassandraConnector,
                      val keyspace: String,
                      val gameRoundResultTable: String,
                      val individualLimitsTable: String,
                      val bigLossEventTable: String) extends Serializable {


  def getIndividualLimits(individualTaxNumber: String): Option[IndividualLimits] = {
    val query: String = individualLimitsQuery(individualTaxNumber)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    var result: IndividualLimits = null

    resultSet.forEach { row =>
      result = IndividualLimits(
        individualTaxNumber = row.getObject(IndividualTaxNumberColumn).asInstanceOf[String],
        betsLimit = getLimitColumnValue(row, BetsLimitColumn),
        betsTimeWindow = getWindowColumnValue(row, BetsTimeWindowColumn),
        depositLimit = getLimitColumnValue(row, DepositLimitColumn),
        depositTimeWindow = getWindowColumnValue(row, DepositTimeWindowColumn),
        lossesLimit = getLimitColumnValue(row, LossesLimitColumn),
        lossesTimeWindow = getWindowColumnValue(row, LossesTimeWindowColumn)
      )
    }

    Option(result)
  }

  private def individualLimitsQuery(individualTaxNumber: String) = {
    s"""
       |SELECT
       | $IndividualTaxNumberColumn,
       | $BetsLimitColumn,
       | $BetsTimeWindowColumn,
       | $DepositLimitColumn,
       | $DepositTimeWindowColumn,
       | $LossesLimitColumn,
       | $LossesTimeWindowColumn
       | FROM $keyspace.$individualLimitsTable
       | WHERE
       | $IndividualTaxNumberColumn = '$individualTaxNumber'
       |""".stripMargin
  }

  private def getLimitColumnValue(row: Row, column: String) = {
    val value = row.getObject(column).asInstanceOf[Double]
    if (value == 0d) None else Option(value)
  }

  private def getWindowColumnValue(row: Row, column: String) = {
    val value = row.getObject(column).asInstanceOf[Int]
    if (value == 0) None else Option(value)
  }

  def getCustomerPlayResultForPeriod(customerId: String, cutOffDateTime: LocalDateTime): Option[IndividualPlayResult] = {

    val query = getCustomerPlayResultQuery(customerId, cutOffDateTime)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    val result = ArrayBuffer[IndividualPlayResult]()
    resultSet.forEach { row =>

      result += IndividualPlayResult(
        individualTaxNumber = row.getObject(IndividualTaxNumberColumn).asInstanceOf[String],
        lossAmount = BigDecimal.valueOf(row.getObject(LossAmountColumn).asInstanceOf[Double]).setScale(2, RoundingMode.UP),
        fromDateTime = Timestamp.from(row.getObject(FromDateTimeColumn).asInstanceOf[Instant]),
        toDateTime = Timestamp.from(row.getObject(ToDateTimeColumn).asInstanceOf[Instant])
      )
    }

    result.headOption
  }

  private def getCustomerPlayResultQuery(individualTaxNumber: String, cutOffDateTime: LocalDateTime): String = {
    s"""
       |SELECT
       | $IndividualTaxNumberColumn,
       | SUM($AmountColumn) as $LossAmountColumn,
       | MIN(result_date) as $FromDateTimeColumn,
       | MAX(result_date) as $ToDateTimeColumn
       | FROM $keyspace.$gameRoundResultTable
       | WHERE
       | $IndividualTaxNumberColumn = '$individualTaxNumber' AND $ResultDateColumn > '${cutOffDateTime.format(TimestampCassandraQueryFormatter)}'
       | GROUP BY $IndividualTaxNumberColumn
       |""".stripMargin
  }

  def saveBigLossEvent(customerPlayResult: IndividualPlayResult): ResultSet = {
    val insertQuery = getInsertBigLossEventQuery(customerPlayResult)
    connector.withSessionDo(session => session.execute(insertQuery))
  }

  private def getInsertBigLossEventQuery(customerPlayResult: IndividualPlayResult): String = {
    s"""
       |INSERT INTO $keyspace.$bigLossEventTable(
       | $IndividualTaxNumberColumn,
       | $FromDateTimeColumn,
       | $ToDateTimeColumn,
       | $EventIdColumn,
       | $LossAmountColumn)
       | VALUES (
       | '${customerPlayResult.individualTaxNumber}',
       | '${customerPlayResult.fromDateTime}',
       | '${customerPlayResult.toDateTime}',
       | '${customerPlayResult.id}',
       | ${customerPlayResult.lossAmount.abs})
       """.stripMargin
  }

  def getCustomerLatestBigLossEventDate(individualTaxNumber: String): Option[LocalDateTime] = {
    val query = getCustomerLatestBigLossEventQuery(individualTaxNumber)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    val result = ArrayBuffer[LocalDateTime]()
    resultSet.forEach { row =>
      result += Timestamp.from(row.getObject(LatestBigLossTimestamp).asInstanceOf[Instant]).toLocalDateTime
    }

    result.headOption
  }

  private def getCustomerLatestBigLossEventQuery(individualTaxNumber: String) = {
    s""" SELECT
       | MAX($ToDateTimeColumn) as latest_big_loss_timestamp
       | FROM $keyspace.$bigLossEventTable
       | WHERE $IndividualTaxNumberColumn = '$individualTaxNumber'
       | GROUP BY $IndividualTaxNumberColumn
       |""".stripMargin
  }
}

object DatabaseService {
  def apply(cassandraConfig: CassandraConfig, sparkConfig: SparkConf): DatabaseService =
    new DatabaseService(
      CassandraConnector(sparkConfig),
      cassandraConfig.keyspace,
      cassandraConfig.gameRoundResultTable,
      cassandraConfig.individualLimitsTable,
      cassandraConfig.bigLossEventTable)
}