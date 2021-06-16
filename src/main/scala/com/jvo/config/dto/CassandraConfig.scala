package com.jvo.config.dto

case class CassandraConfig(host: String,
                           keyspace: String,
                           individualLimitsTable: String,
                           gameRoundResultTable: String,
                           bigLossEventTable: String)