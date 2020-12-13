package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.SingleSrcTransformationOptions

case class WithColumnTransformationOptions(override val transformationType: String,
                                           override val inputSourceId: String,
                                           columns: List[ColumnOption])
  extends SingleSrcTransformationOptions(transformationType, inputSourceId)

object WithColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[WithColumnTransformationOptions] = DecodeJson.derive[WithColumnTransformationOptions]
}

case class ColumnOption(expression: String, alias: String)

object ColumnOption {

  implicit def decodeJson: DecodeJson[ColumnOption] = DecodeJson.derive[ColumnOption]
}

