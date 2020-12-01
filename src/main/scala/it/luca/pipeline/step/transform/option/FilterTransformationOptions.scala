package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.SingleSrcTransformationOptions

case class FilterTransformationOptions(override val transformationType: String,
                                       override val inputSourceId: String,
                                       filterCondition: String)
  extends SingleSrcTransformationOptions(transformationType, inputSourceId)

object FilterTransformationOptions {

  implicit def decodeJson: DecodeJson[FilterTransformationOptions] = DecodeJson.derive[FilterTransformationOptions]
}
