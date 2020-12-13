package it.luca.pipeline.step.transform.option

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.common.MultipleSrcTransformationOptions

case class UnionTransformationOptions(override val transformationType: String,
                                      inputSourceIds: List[String])
  extends MultipleSrcTransformationOptions(transformationType)

object UnionTransformationOptions {

  implicit def decodeJson: DecodeJson[UnionTransformationOptions] = DecodeJson.derive[UnionTransformationOptions]
}
