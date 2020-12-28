package it.luca.pipeline.step.transform

import argonaut.DecodeJson

case class WithColumnTransformationOptions(override val transformationType: String, override val transformationOrder: Int, columns: List[ColumnOption])
  extends TransformationOptions(transformationType, transformationOrder)

object WithColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[WithColumnTransformationOptions] = DecodeJson.derive[WithColumnTransformationOptions]
}

case class ColumnOption(expression: String, alias: String)

object ColumnOption {

  implicit def decodeJson: DecodeJson[ColumnOption] = DecodeJson.derive[ColumnOption]
}

