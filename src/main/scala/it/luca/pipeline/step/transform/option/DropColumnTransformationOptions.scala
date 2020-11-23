package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class DropColumnTransformationOptions(override val transformationType: String,
                                           override val inputSourceId: String,
                                           columns: List[String])
  extends SingleSourceTransformationOptions(transformationType, inputSourceId)

object DropColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[DropColumnTransformationOptions] = DecodeJson.derive[DropColumnTransformationOptions]
}
