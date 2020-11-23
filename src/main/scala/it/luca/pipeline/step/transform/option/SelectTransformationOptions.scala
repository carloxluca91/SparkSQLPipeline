package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson

case class SelectTransformationOptions(override val transformationType: String,
                                       override val inputSourceId: String,
                                       columns: List[String])
  extends SingleSourceTransformationOptions(transformationType, inputSourceId)

object SelectTransformationOptions {

  implicit def decodeJson: DecodeJson[SelectTransformationOptions] = DecodeJson.derive[SelectTransformationOptions]
}
