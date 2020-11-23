package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class ColumnOption(expression: String, alias: String)

object ColumnOption {

  implicit def decodeJson: DecodeJson[ColumnOption] = DecodeJson.derive[ColumnOption]
}
