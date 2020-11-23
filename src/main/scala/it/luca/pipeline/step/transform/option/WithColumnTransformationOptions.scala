package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class WithColumnTransformationOptions(override val transformationType: String,
                                           override val inputSourceId: String,
                                           columns: List[ColumnOption])
  extends SingleSourceTransformationOptions(transformationType, inputSourceId)

object WithColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[WithColumnTransformationOptions] = DecodeJson.derive[WithColumnTransformationOptions]
}
