package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.SingleSrcTransformationOptions

case class SelectTransformationOptions(override val transformationType: String,
                                       override val inputSourceId: String,
                                       columns: List[String])
  extends SingleSrcTransformationOptions(transformationType, inputSourceId)

object SelectTransformationOptions {

  implicit def decodeJson: DecodeJson[SelectTransformationOptions] = DecodeJson.derive[SelectTransformationOptions]
}
