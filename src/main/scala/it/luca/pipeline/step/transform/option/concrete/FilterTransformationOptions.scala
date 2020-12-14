package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.SingleSrcTransformationOptions

case class FilterTransformationOptions(override val transformationType: String,
                                       override val inputDfId: String,
                                       filterCondition: String)
  extends SingleSrcTransformationOptions(transformationType, inputDfId)

object FilterTransformationOptions {

  implicit def decodeJson: DecodeJson[FilterTransformationOptions] = DecodeJson.derive[FilterTransformationOptions]
}
