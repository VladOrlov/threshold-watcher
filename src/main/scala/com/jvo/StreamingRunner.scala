package com.jvo

import com.jvo.config.Constants.EventDateFormatter
import com.jvo.config.dto.{ApplicationConfiguration, KafkaConfig, SparkConfig}
import com.jvo.config.{ConfigurableStreamingApp, KafkaComponent, SparkComponent}
import com.jvo.dto._
import com.jvo.service.{DatabaseService, ThresholdsChecker}
import com.jvo.utils.JsonObjectMapper
import com.jvo.utils.kafka.KafkaSink
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast

import java.time.{LocalDateTime, ZoneOffset}


object StreamingRunner extends ConfigurableStreamingApp with KafkaComponent with SparkComponent {

  private[this] final val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val applicationConfiguration = loadConfiguration()
    val ApplicationConfiguration(kafkaConfig, sparkConfig, cassandraConfig) = applicationConfiguration

    val ssc = initSparkStreamingContext(applicationConfiguration)
    val kafkaInputStream = getKafkaInputStream(ssc, applicationConfiguration)

    implicit val kafkaConfigBroadcast: Broadcast[KafkaConfig] = ssc.sparkContext.broadcast(kafkaConfig)
    implicit val sparkConfigBroadcast: Broadcast[SparkConfig] = ssc.sparkContext.broadcast(sparkConfig)
    implicit val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink(kafkaConfig.bootstrapServers))
    val databaseService = DatabaseService(cassandraConfig, ssc.sparkContext.getConf)
    val gameRoundSettler = ThresholdsChecker(databaseService)
    implicit val gameRoundSettlerBroadcast: Broadcast[ThresholdsChecker] = ssc.sparkContext.broadcast(gameRoundSettler)
    //    implicit val cassandraRepository: Broadcast[DatabaseService] = ssc.sparkContext.broadcast(databaseService)

    kafkaInputStream
      .map(message => (message.key(), JsonObjectMapper.parseToLossCheckEvent(message.value())))
      .groupByKey(sparkConfig.partitionsNumber)
      .flatMap(groupedEvents => gameRoundSettlerBroadcast.value.processGameRoundFinalTransaction(getLatestLossCheckEvent(groupedEvents)))
      .foreachRDD(rdd => rdd.foreach { lossEvent: ThresholdEvent =>
        sendMessageToKafkaTopic(lossEvent)
      })

    ssc.start()
    ssc.awaitTermination()
  }

  private def getLatestLossCheckEvent(groupedEvents: (String, Iterable[LossCheckEvent])): CheckIndividualThresholds = {
    val tuple = groupedEvents._2
      .map(lossCheckEvent => (convertToDate(lossCheckEvent), lossCheckEvent))
      .maxBy(_._1)

    CheckIndividualThresholds(tuple._2.individualTaxNumber, LocalDateTime.ofInstant(tuple._1, ZoneOffset.UTC))
  }

  private def convertToDate(lossCheckEvent: LossCheckEvent) = {
    LocalDateTime.parse(lossCheckEvent.checkDateTime, EventDateFormatter).toInstant(ZoneOffset.UTC)
  }

  private def sendMessageToKafkaTopic(bigLossEvent: ThresholdEvent)
                                     (implicit kafkaConfigBroadcast: Broadcast[KafkaConfig],
                                      kafkaSinkBroadcast: Broadcast[KafkaSink]): Unit = {

    publishToKafkaTopic(kafkaSinkBroadcast)(
      getDestinationTopic,
      bigLossEvent.individualTaxNumber,
      JsonObjectMapper.toJson(bigLossEvent))
  }

  private def publishToKafkaTopic(kafkaSinkBroadcast: Broadcast[KafkaSink]) = {
    kafkaSinkBroadcast.value.send(_: String, _: String, _: String)
  }

  private def getDestinationTopic(implicit kafkaConfigBroadcast: Broadcast[KafkaConfig]) = {
    kafkaConfigBroadcast.value.topics.destination
  }


}
