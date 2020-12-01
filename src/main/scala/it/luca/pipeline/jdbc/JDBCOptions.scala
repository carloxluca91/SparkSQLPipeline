package it.luca.pipeline.jdbc

import argonaut.DecodeJson

case class JDBCOptions(jdbcUrl: String,
                       jdbcDriver: String,
                       jdbcUser: String,
                       jdbcPassword: String,
                       jdbcUseSSL: Option[String])

object JDBCOptions {

  implicit def decodeJson: DecodeJson[JDBCOptions] = DecodeJson.derive[JDBCOptions]
}
