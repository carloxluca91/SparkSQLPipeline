package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class WithColumnRenamedOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[WithColumnRenamedOption])
  extends TransformationOptions(transformationType, transformationOrder)

object WithColumnRenamedOptions {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOptions] = DecodeJson.derive[WithColumnRenamedOptions]
}

case class WithColumnRenamedOption(oldName: String, newName: String)

object WithColumnRenamedOption {

  implicit def decodeJson: DecodeJson[WithColumnRenamedOption] = DecodeJson.derive[WithColumnRenamedOption]
}