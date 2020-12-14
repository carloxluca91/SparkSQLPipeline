package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.SingleSrcTransformationOptions

case class SelectTransformationOptions(override val transformationType: String,
                                       override val inputDfId: String,
                                       columns: List[String])
  extends SingleSrcTransformationOptions(transformationType, inputDfId)

object SelectTransformationOptions {

  implicit def decodeJson: DecodeJson[SelectTransformationOptions] = DecodeJson.derive[SelectTransformationOptions]
}
