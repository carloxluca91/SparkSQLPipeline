package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class WithColumnRenamedOption(oldName: String, newName: String)

object WithColumnRenamedOption {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOption] = DecodeJson.derive[WithColumnRenamedOption]
}
