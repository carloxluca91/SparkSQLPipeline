package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class JoinSelectColumn(side: String,
                            expression: String,
                            alias: Option[String])

object JoinSelectColumn {

  implicit def decodeJson: DecodeJson[JoinSelectColumn] = DecodeJson.derive[JoinSelectColumn]
}
