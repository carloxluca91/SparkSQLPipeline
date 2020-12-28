package it.luca.pipeline.step.transform.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.transform.option.common.MultipleSrcTransformationOptions

case class UnionTransformationOptions(override val transformationType: String,
                                      override val inputDfIds: List[String])
  extends MultipleSrcTransformationOptions[List[String]](transformationType, inputDfIds)

object UnionTransformationOptions {

  implicit def decodeJson: DecodeJson[UnionTransformationOptions] = DecodeJson.derive[UnionTransformationOptions]
}
