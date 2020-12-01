package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.SingleSrcTransformationOptions

case class DropColumnTransformationOptions(override val transformationType: String,
                                           override val inputSourceId: String,
                                           columns: List[String])
  extends SingleSrcTransformationOptions(transformationType, inputSourceId)

object DropColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[DropColumnTransformationOptions] = DecodeJson.derive[DropColumnTransformationOptions]
}
