package com.jvo.utils

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.jvo.dto.LossCheckEvent
import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}

object JsonObjectMapper {

  private[this] final val log = LogManager.getLogger(this.getClass)

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)


  def parseToLossCheckEvent(sourceJson: String): LossCheckEvent = {
    Try(mapper.readValue[LossCheckEvent](sourceJson)) match {
      case Success(v) => v
      case Failure(e) =>
        log.error(e.getMessage)
        throw new RuntimeException(e)
    }
  }

  def toJson(sourceObject: AnyRef): String = {
    mapper.writeValueAsString(sourceObject)
  }
}