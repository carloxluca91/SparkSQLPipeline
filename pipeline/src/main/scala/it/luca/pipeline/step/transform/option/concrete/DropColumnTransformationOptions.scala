package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.SingleSrcTransformationOptions

case class DropColumnTransformationOptions(override val transformationType: String,
                                           override val inputDfId: String,
                                           columns: List[String])
  extends SingleSrcTransformationOptions(transformationType, inputDfId)

object DropColumnTransformationOptions {

  implicit def decodeJson: DecodeJson[DropColumnTransformationOptions] = DecodeJson.derive[DropColumnTransformationOptions]
}
